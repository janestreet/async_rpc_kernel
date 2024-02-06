open Core
open Async_kernel
module Time_ns = Core_private.Time_ns_alternate_sexp
module P = Protocol
module Reader = Transport.Reader
module Writer = Transport.Writer

module Handshake_error = struct
  module T = struct
    type t =
      | Eof
      | Transport_closed
      | Timeout
      | Reading_header_failed of Error.t
      | Negotiation_failed of Error.t
      | Negotiated_unexpected_version of int
    [@@deriving sexp]
  end

  include T
  include Sexpable.To_stringable (T)

  exception Handshake_error of (t * Info.t) [@@deriving sexp]

  let to_exn ~connection_description t = Handshake_error (t, connection_description)
end

module Header : sig
  type t [@@deriving bin_io, sexp_of]

  val v1 : t
  val v2 : t
  val v3 : t
  val negotiate : us:t -> peer:t -> (int, Handshake_error.t) result
end = struct
  include P.Header

  let create ~supported_versions =
    Protocol_version_header.create_exn () ~protocol:Rpc ~supported_versions
  ;;

  let v3 = create ~supported_versions:[ 1; 2; 3 ]
  let v2 = create ~supported_versions:[ 1; 2 ]
  let v1 = create ~supported_versions:[ 1 ]

  let negotiate ~us ~peer =
    match negotiate ~allow_legacy_peer:true ~us ~peer with
    | Error e -> Error (Handshake_error.Negotiation_failed e)
    | Ok i -> Ok i
  ;;
end

module Peer_metadata = struct
  type t =
    | Unsupported
    | Expected of
        (Bigstring.t option * Menu.t option, [ `Connection_closed ]) Result.t Ivar.t
  [@@deriving sexp_of]
end

module Heartbeat_config = struct
  type t =
    { timeout : Time_ns.Span.t
    ; send_every : Time_ns.Span.t
    }
  [@@deriving sexp, bin_io, fields ~getters]

  let%expect_test _ =
    print_endline [%bin_digest: t];
    [%expect {| 74a1f475bfb2eed5a509ba71cd7891d2 |}]
  ;;

  let create
    ?(timeout = Time_ns.Span.of_sec 30.)
    ?(send_every = Time_ns.Span.of_sec 10.)
    ()
    =
    { timeout; send_every }
  ;;

  module Runtime = struct
    type t =
      { mutable timeout : Time_ns.Span.t
      ; send_every : Time_ns.Span.t
      }
    [@@deriving sexp_of]
  end

  let to_runtime { timeout; send_every } = { Runtime.timeout; send_every }
end

type response_handler =
  Nat0.t P.Response.t
  -> read_buffer:Bigstring.t
  -> read_buffer_pos_ref:int ref
  -> [ `keep
     | `wait of unit Deferred.t
     | `remove of unit Rpc_result.t
     | `remove_and_wait of unit Deferred.t
     ]

type t =
  { description : Info.t
  ; heartbeat_config : Heartbeat_config.Runtime.t
  ; mutable heartbeat_callbacks : (unit -> unit) array
  ; mutable last_seen_alive : Time_ns.t
  ; max_metadata_size : Byte_units.t
  ; reader : Reader.t
  ; writer : Protocol_writer.t
  ; open_queries : (P.Query_id.t, (response_handler[@sexp.opaque])) Hashtbl.t
  ; close_started : Info.t Ivar.t
  ; close_finished : unit Ivar.t
      (* There's a circular dependency between connections and their implementation instances
     (the latter depends on the connection state, which is given access to the connection
     when it is created). *)
  ; implementations_instance : Implementations.Instance.t Set_once.t
  ; time_source : Synchronous_time_source.t
  ; heartbeat_event : Synchronous_time_source.Event.t Set_once.t
  ; negotiated_protocol_version : int Set_once.t
  ; events : (Tracing_event.t -> unit) Bus.Read_write.t
      (* Variant is decided once the protocol version negotiation is completed -- then, either
     sending the id is unsupported, or the id is requested and is on its way or received
  *)
  ; peer_metadata : Peer_metadata.t Set_once.t
      (* responses to queries are written by the implementations instance. Other events
     are written by this module. *)
  }
[@@deriving sexp_of]

let sexp_of_t_hum_writer t =
  [%sexp
    { description : Info.t = t.description; writer : Protocol_writer.writer = t.writer }]
;;

let description t = t.description
let is_closed t = Ivar.is_full t.close_started

let map_metadata t ~kind_of_metadata ~f =
  match Set_once.get t.peer_metadata with
  | Some Unsupported -> f (Error `Unsupported)
  | None -> f (Error `No_handshake)
  | Some (Expected result) ->
    (match%bind.Eager_deferred Ivar.read result with
     | Ok md -> f (Ok md)
     | Error `Connection_closed ->
       f
         (Error
            (`Closed
              [%lazy_message
                "Connection closed before we could get peer metadata"
                  ~trying_to_get:kind_of_metadata
                  ~connection_description:(t.description : Info.t)
                  ~close_reason:(Ivar.peek t.close_started : Info.t option)])))
;;

let peer_identification t =
  map_metadata t ~kind_of_metadata:"peer_identification" ~f:(function
    | Ok (id, (_ : Menu.t option)) -> return id
    | Error `Unsupported | Error `No_handshake | Error (`Closed (_ : Sexp.t lazy_t)) ->
      return None)
;;

let peer_menu t =
  map_metadata t ~kind_of_metadata:"peer_menu" ~f:(function
    | Ok ((_ : Bigstring.t option), menu) -> return (Ok menu)
    | Error `Unsupported | Error `No_handshake -> return (Ok None)
    | Error (`Closed info) -> return (Error (Error.of_lazy_sexp info)))
;;

let peer_menu' t =
  map_metadata t ~kind_of_metadata:"peer_menu" ~f:(function
    | Ok ((_ : Bigstring.t option), menu) -> return (Ok menu)
    | Error `Unsupported | Error `No_handshake -> return (Ok None)
    | Error (`Closed (_ : Sexp.t lazy_t)) -> return (Error Rpc_error.Connection_closed))
;;

let writer t =
  if is_closed t || not (Protocol_writer.can_send t.writer)
  then Error `Closed
  else Ok t.writer
;;

let bytes_to_write t = Protocol_writer.bytes_to_write t.writer
let bytes_written t = Protocol_writer.bytes_written t.writer
let bytes_read t = Reader.bytes_read t.reader
let flushed t = Protocol_writer.flushed t.writer
let events t = (t.events :> _ Bus.Read_only.t)

let write_event t event =
  if not (Bus.is_closed t.events) then Bus.write_local t.events event
;;

module Dispatch_error = struct
  type t =
    | Closed
    | Message_too_big of Transport.Send_result.message_too_big
  [@@deriving sexp_of]
end

let handle_send_result :
      'a.
      t
      -> 'a Transport.Send_result.t
      -> rpc:Description.t
      -> id:P.Query_id.t
      -> ('a, Dispatch_error.t) Result.t
  =
  fun t r ~rpc ~id ->
  let id = (id :> Int63.t) in
  match r with
  | Sent { result = x; bytes } ->
    write_event t { event = Sent Query; rpc = Some rpc; id; payload_bytes = bytes };
    Ok x
  | Closed ->
    write_event
      t
      { event = Failed_to_send (Query, Closed); rpc = Some rpc; id; payload_bytes = 0 };
    Error Dispatch_error.Closed
  | Message_too_big err ->
    write_event
      t
      { event = Failed_to_send (Query, Too_large)
      ; rpc = Some rpc
      ; id
      ; payload_bytes = err.size
      };
    Error
      (Dispatch_error.Message_too_big
         ([%globalize: Transport.Send_result.message_too_big] err))
;;

(* Used for heartbeats and protocol negotiation *)
let handle_special_send_result t (result : unit Transport.Send_result.t) =
  match result with
  | Sent { result = (); bytes = (_ : int) } -> ()
  | Closed ->
    failwiths ~here:[%here] "RPC connection got closed writer" t sexp_of_t_hum_writer
  | Message_too_big _ ->
    raise_s
      [%sexp
        "Message cannot be sent"
        , { reason =
              ([%globalize: unit Transport.Send_result.t] result
                : unit Transport.Send_result.t)
          ; connection = (t : t_hum_writer)
          }]
;;

let send_query_with_registered_response_handler
  t
  (query : 'query P.Query.t)
  ~response_handler
  ~(send_query : 'query P.Query.t -> 'response Transport.Send_result.t)
  : ('response, Dispatch_error.t) Result.t
  =
  let registered_response_handler =
    match response_handler with
    | Some response_handler ->
      Hashtbl.set t.open_queries ~key:query.id ~data:response_handler;
      true
    | None -> false
  in
  let result =
    send_query query
    |> handle_send_result
         t
         ~rpc:{ name = P.Rpc_tag.to_string query.tag; version = query.version }
         ~id:query.id
  in
  if registered_response_handler && Result.is_error result
  then Hashtbl.remove t.open_queries query.id;
  result
;;

let dispatch t ~response_handler ~bin_writer_query ~(query : _ P.Query.t) =
  match writer t with
  | Error `Closed -> Error Dispatch_error.Closed
  | Ok writer ->
    let query =
      match query.metadata with
      | Some metadata ->
        { query with
          metadata =
            Some (String.prefix metadata (Byte_units.bytes_int_exn t.max_metadata_size))
        }
      | None -> query
    in
    send_query_with_registered_response_handler
      t
      query
      ~response_handler
      ~send_query:(fun query -> Protocol_writer.send_query writer query ~bin_writer_query) 
    [@nontail]
;;

let make_dispatch_bigstring
  do_send
  ?metadata
  t
  ~tag
  ~version
  buf
  ~pos
  ~len
  ~response_handler
  =
  match writer t with
  | Error `Closed -> Error Dispatch_error.Closed
  | Ok writer ->
    let id = P.Query_id.create () in
    let query : unit P.Query.t = { tag; version; id; metadata; data = () } in
    send_query_with_registered_response_handler
      t
      query
      ~response_handler
      ~send_query:(fun query ->
      Protocol_writer.send_expert_query
        writer
        query
        ~buf
        ~pos
        ~len
        ~send_bin_prot_and_bigstring:do_send) [@nontail]
;;

let dispatch_bigstring = make_dispatch_bigstring Writer.send_bin_prot_and_bigstring

let schedule_dispatch_bigstring =
  make_dispatch_bigstring Writer.send_bin_prot_and_bigstring_non_copying
;;

let handle_response
  t
  (response : Nat0.t P.Response.t)
  ~read_buffer
  ~read_buffer_pos_ref
  ~protocol_message_len
  : _ Transport.Handler_result.t
  =
  match Hashtbl.find t.open_queries response.id with
  | None -> Stop (Error (Rpc_error.Unknown_query_id response.id))
  | Some response_handler ->
    let payload_bytes =
      protocol_message_len
      +
      match response.data with
      | Ok x -> (x :> int)
      | Error _ -> 0
    in
    let response_event kind ~payload_bytes =
      write_event
        t
        { Tracing_event.event = Received (Response kind)
        ; rpc = None
        ; id = (response.id :> Int63.t)
        ; payload_bytes
        };
      ()
    in
    (match response_handler response ~read_buffer ~read_buffer_pos_ref with
     | `keep ->
       response_event Partial_response ~payload_bytes;
       Continue
     | `wait wait ->
       response_event Partial_response ~payload_bytes;
       Wait wait
     | `remove_and_wait wait ->
       response_event Response_finished ~payload_bytes;
       Hashtbl.remove t.open_queries response.id;
       Wait wait
     | `remove removal_circumstances ->
       response_event Response_finished ~payload_bytes;
       Hashtbl.remove t.open_queries response.id;
       (match removal_circumstances with
        | Ok () -> Continue
        | Error e ->
          (match e with
           | Unimplemented_rpc _ -> Continue
           | Bin_io_exn _
           | Connection_closed
           | Write_error _
           | Uncaught_exn _
           | Unknown_query_id _
           | Authorization_failure _
           | Message_too_big _
           | Unknown _ -> Stop (Error e))))
;;

let handle_msg
  t
  (msg : _ P.Message.t)
  ~read_buffer
  ~read_buffer_pos_ref
  ~close_connection_monitor
  ~protocol_message_len
  : _ Transport.Handler_result.t
  =
  match msg with
  | Metadata metadata ->
    (match Set_once.get t.peer_metadata with
     | None | Some Unsupported ->
       raise_s
         [%message
           "Inconsistent state: receiving a metadata message is unsupported, but a \
            metadata message was received"]
     | Some (Expected ivar) ->
       if Ivar.is_empty t.close_started
       then
         Ivar.fill_exn
           ivar
           (Ok (metadata.identification, Option.map metadata.menu ~f:Menu.of_v2_response)));
    Continue
  | Heartbeat ->
    Array.iter t.heartbeat_callbacks ~f:(fun f -> f ());
    Continue
  | Response response ->
    handle_response t response ~read_buffer ~read_buffer_pos_ref ~protocol_message_len
  | Query query ->
    let instance = Set_once.get_exn t.implementations_instance [%here] in
    write_event
      t
      { event = Received Query
      ; rpc = Some { name = P.Rpc_tag.to_string query.tag; version = query.version }
      ; id = (query.id :> Int63.t)
      ; payload_bytes = protocol_message_len + (query.data :> int)
      };
    Implementations.Instance.handle_query
      instance
      ~close_connection_monitor
      ~query
      ~read_buffer
      ~read_buffer_pos_ref
  | Query_v1 query ->
    let instance = Set_once.get_exn t.implementations_instance [%here] in
    write_event
      t
      { event = Received Query
      ; rpc = Some { name = P.Rpc_tag.to_string query.tag; version = query.version }
      ; id = (query.id :> Int63.t)
      ; payload_bytes = protocol_message_len + (query.data :> int)
      };
    let query = P.Query.of_v1 query in
    Implementations.Instance.handle_query
      instance
      ~close_connection_monitor
      ~query
      ~read_buffer
      ~read_buffer_pos_ref
;;

let close_reason t ~on_close =
  let reason = Ivar.read t.close_started in
  match on_close with
  | `started -> reason
  | `finished ->
    let%bind () = Ivar.read t.close_finished in
    reason
;;

let close_finished t = Ivar.read t.close_finished

let add_heartbeat_callback t f =
  (* Adding heartbeat callbacks is relatively rare, but the callbacks are triggered a lot.
     The array representation makes the addition quadratic for the sake of keeping the
     triggering cheap. *)
  t.heartbeat_callbacks <- Array.append [| f |] t.heartbeat_callbacks
;;

let reset_heartbeat_timeout t timeout =
  t.heartbeat_config.timeout <- timeout;
  t.last_seen_alive <- Synchronous_time_source.now t.time_source
;;

let last_seen_alive t = t.last_seen_alive

let abort_heartbeating t =
  Option.iter (Set_once.get t.heartbeat_event) ~f:(fun event ->
    match Synchronous_time_source.Event.abort t.time_source event with
    | Ok | Previously_unscheduled -> ())
;;

let close ?(streaming_responses_flush_timeout = Time_ns.Span.of_int_sec 5) ~reason t =
  if not (is_closed t)
  then (
    abort_heartbeating t;
    Ivar.fill_exn t.close_started reason;
    (match Set_once.get t.implementations_instance with
     | None -> Deferred.unit
     | Some instance ->
       let flushed = Implementations.Instance.flush instance in
       if Deferred.is_determined flushed
       then (
         Implementations.Instance.stop instance;
         flushed)
       else (
         let%map () =
           Deferred.any_unit
             [ flushed
             ; Protocol_writer.stopped t.writer
             ; Time_source.after
                 (Time_source.of_synchronous t.time_source)
                 streaming_responses_flush_timeout
             ]
         in
         Implementations.Instance.stop instance))
    >>> fun () ->
    Protocol_writer.close t.writer
    >>> fun () -> Reader.close t.reader >>> fun () -> Ivar.fill_exn t.close_finished ());
  close_finished t
;;

let on_message t ~close_connection_monitor =
  let f buf ~pos ~len:_ : _ Transport.Handler_result.t =
    let pos_ref = ref pos in
    let nat0_msg = P.Message.bin_read_nat0_t buf ~pos_ref in
    match
      handle_msg
        t
        nat0_msg
        ~read_buffer:buf
        ~read_buffer_pos_ref:pos_ref
        ~close_connection_monitor
        ~protocol_message_len:(!pos_ref - pos)
    with
    | Continue -> Continue
    | Wait _ as res -> res
    | Stop result ->
      let reason =
        let msg = "Rpc message handling loop stopped" in
        match result with
        | Ok () -> Info.of_string msg
        | Error e ->
          Info.create
            msg
            e
            (Rpc_error.sexp_of_t ~get_connection_close_reason:(fun () ->
               [%sexp
                 "Connection.on_message resulted in Connection_closed error. This is \
                  weird."]))
      in
      don't_wait_for (close t ~reason);
      Stop reason
  in
  Staged.stage f
;;

let heartbeat_now t =
  let since_last_heartbeat =
    Time_ns.diff (Synchronous_time_source.now t.time_source) t.last_seen_alive
  in
  if Time_ns.Span.( > ) since_last_heartbeat t.heartbeat_config.timeout
  then (
    let reason () =
      sprintf
        !"No heartbeats received for %{sexp:Time_ns.Span.t}."
        t.heartbeat_config.timeout
    in
    don't_wait_for (close t ~reason:(Info.of_thunk reason)))
  else (
    match writer t with
    | Error `Closed -> ()
    | Ok writer ->
      Protocol_writer.send_heartbeat writer |> handle_special_send_result t;
      ())
;;

let default_handshake_timeout = Time_ns.Span.of_sec 30.

let cleanup t ~reason exn =
  don't_wait_for (close ~reason t);
  if not (Hashtbl.is_empty t.open_queries)
  then (
    let error =
      match exn with
      | Rpc_error.Rpc (error, (_ : Info.t)) -> error
      | exn -> Uncaught_exn (Exn.sexp_of_t exn)
    in
    (* clean up open streaming responses *)
    (* an unfortunate hack; ok because the response handler will have nothing
       to read following a response where [data] is an error *)
    let dummy_buffer = Bigstring.create 1 in
    let dummy_ref = ref 0 in
    Hashtbl.iteri t.open_queries ~f:(fun ~key:query_id ~data:response_handler ->
      ignore
        (response_handler
           ~read_buffer:dummy_buffer
           ~read_buffer_pos_ref:dummy_ref
           { id = query_id; data = Error error }));
    Hashtbl.clear t.open_queries;
    Bigstring.unsafe_destroy dummy_buffer)
;;

let schedule_heartbeats t =
  t.last_seen_alive <- Synchronous_time_source.now t.time_source;
  let heartbeat_from_now_on =
    (* [at_intervals] will schedule the first heartbeat the first time the time_source is
       advanced *)
    Synchronous_time_source.Event.at_intervals
      t.time_source
      t.heartbeat_config.send_every
      (fun () -> heartbeat_now t)
  in
  Set_once.set_exn t.heartbeat_event [%here] heartbeat_from_now_on
;;

let run_after_handshake
  t
  ~negotiated_protocol_version
  ~implementations
  ~connection_state
  ~writer_monitor_exns
  =
  Protocol_writer.set_negotiated_protocol_version t.writer negotiated_protocol_version;
  let instance =
    Implementations.instantiate
      implementations
      ~writer:t.writer
      ~events:t.events
      ~connection_description:t.description
      ~connection_close_started:(Ivar.read t.close_started)
      ~connection_state:(connection_state t)
  in
  Set_once.set_exn t.implementations_instance [%here] instance;
  let close_connection_monitor = Monitor.create ~name:"RPC close connection monitor" () in
  Monitor.detach_and_iter_errors close_connection_monitor ~f:(fun exn ->
    let reason =
      Info.create_s [%message "Uncaught exception in implementation" (exn : Exn.t)]
    in
    don't_wait_for (close ~reason t));
  let monitor = Monitor.create ~name:"RPC connection loop" () in
  let reason name exn =
    exn, Info.tag (Info.of_exn exn) ~tag:("exn raised in RPC connection " ^ name)
  in
  Stream.iter
    (Stream.interleave
       (Stream.of_list
          [ Stream.map ~f:(reason "loop") (Monitor.detach_and_get_error_stream monitor)
          ; Stream.map ~f:(reason "Writer.t") writer_monitor_exns
          ]))
    ~f:(fun (exn, reason) -> cleanup t exn ~reason);
  within ~monitor (fun () ->
    schedule_heartbeats t;
    Reader.read_forever
      t.reader
      ~on_message:(Staged.unstage (on_message t ~close_connection_monitor))
      ~on_end_of_batch:(fun () ->
        t.last_seen_alive <- Synchronous_time_source.now t.time_source)
    >>> function
    | Ok reason -> cleanup t ~reason (Rpc_error.Rpc (Connection_closed, t.description))
    (* The protocol is such that right now, the only outcome of the other side closing the
       connection normally is that we get an eof. *)
    | Error (`Eof | `Closed) ->
      cleanup
        t
        ~reason:(Info.of_string "EOF or connection closed")
        (Rpc_error.Rpc (Connection_closed, t.description)))
;;

let send_metadata t writer ~menu ?identification () =
  P.Message.Metadata { identification; menu }
  |> Writer.send_bin_prot
       writer
       (P.Message.bin_writer_maybe_needs_length P.Connection_metadata.V1.bin_t.writer)
  |> handle_special_send_result t;
  ()
;;

let negotiate t ?identification ~header ~peer ~writer ~menu () =
  let negotiate_result = Header.negotiate ~us:header ~peer in
  match negotiate_result with
  | Ok version ->
    if version >= 3
    then (
      let ivar = Ivar.create () in
      upon (Ivar.read t.close_started) (fun (_ : Info.t) ->
        Ivar.fill_if_empty ivar (Error `Connection_closed));
      Set_once.set_exn t.peer_metadata [%here] (Expected ivar);
      send_metadata t writer ~menu ?identification ())
    else Set_once.set_exn t.peer_metadata [%here] Unsupported;
    return negotiate_result
  | Error (_ : Handshake_error.t) -> return negotiate_result
;;

let do_handshake t writer ~handshake_timeout ~header ~menu ?identification () =
  if not (Writer.can_send writer)
  then return (Error Handshake_error.Transport_closed)
  else (
    Writer.send_bin_prot writer Header.bin_t.writer header |> handle_special_send_result t;
    (* If we use [max_connections] in the server, then this read may just hang until the
       server starts accepting new connections (which could be never).  That is why a
       timeout is used *)
    let result =
      Monitor.try_with ~rest:`Log ~run:`Now (fun () ->
        Reader.read_one_message_bin_prot t.reader Header.bin_t.reader)
    in
    match%bind
      Time_source.with_timeout
        (Time_source.of_synchronous t.time_source)
        handshake_timeout
        result
    with
    | `Timeout ->
      (* There's a pending read, the reader is basically useless now, so we clean it
         up. *)
      don't_wait_for (close t ~reason:(Info.of_string "Handshake timeout"));
      return (Error Handshake_error.Timeout)
    | `Result (Error exn) ->
      let reason = Info.of_string "[Reader.read_one_message_bin_prot] raised" in
      don't_wait_for (close t ~reason);
      return (Error (Handshake_error.Reading_header_failed (Error.of_exn exn)))
    | `Result (Ok (Error `Eof)) -> return (Error Handshake_error.Eof)
    | `Result (Ok (Error `Closed)) -> return (Error Handshake_error.Transport_closed)
    | `Result (Ok (Ok peer)) -> negotiate t ?identification ~writer ~peer ~header ~menu ())
;;

let contains_magic_prefix = Protocol_version_header.contains_magic_prefix ~protocol:Rpc
let default_handshake_header = Header.v3

let handshake_header_override_key =
  Univ_map.Key.create ~name:"async rpc handshake header override" [%sexp_of: Header.t]
;;

let get_handshake_header () =
  Async_kernel_scheduler.find_local handshake_header_override_key
  |> Option.value ~default:default_handshake_header
;;

let create
  ?implementations
  ~connection_state
  ?(handshake_timeout = default_handshake_timeout)
  ?(heartbeat_config = Heartbeat_config.create ())
  ?(max_metadata_size = Byte_units.of_kilobytes 1.)
  ?(description = Info.of_string "<created-directly>")
  ?(time_source = Synchronous_time_source.wall_clock ())
  ?identification
  ({ reader; writer } : Transport.t)
  =
  let implementations =
    match implementations with
    | None -> Implementations.null ()
    | Some s -> s
  in
  let t =
    { description
    ; heartbeat_config = Heartbeat_config.to_runtime heartbeat_config
    ; heartbeat_callbacks = [||]
    ; last_seen_alive = Synchronous_time_source.now time_source
    ; max_metadata_size
    ; reader
    ; writer = Protocol_writer.create_before_negotiation writer
    ; open_queries = Hashtbl.Poly.create ~size:10 ()
    ; close_started = Ivar.create ()
    ; close_finished = Ivar.create ()
    ; implementations_instance = Set_once.create ()
    ; time_source
    ; heartbeat_event = Set_once.create ()
    ; negotiated_protocol_version = Set_once.create ()
    ; events =
        Bus.create_exn
          [%here]
          Arity1_local
          ~on_subscription_after_first_write:Allow
          ~on_callback_raise:Error.raise
    ; peer_metadata = Set_once.create ()
    }
  in
  let writer_monitor_exns = Monitor.detach_and_get_error_stream (Writer.monitor writer) in
  upon (Writer.stopped writer) (fun () ->
    don't_wait_for (close t ~reason:(Info.of_string "RPC transport stopped")));
  upon (Ivar.read t.close_finished) (fun () -> Bus.close t.events);
  let header = get_handshake_header () in
  let menu, implementations =
    (* There are three cases:

       1. [Versioned_rpc.Menu.add] inserted the default menu
       2. [Versioned_rpc.Menu.implement_multi] inserted a custom menu
       3. No menu was inserted into the implementations

       We need to be able to distinguish between the three to preserve behaviour for old
       clients so we have modified [Versioned_rpc.Menu.add] that inserts a special
       implementation type that allows us to precompute the menu. *)
    match
      Implementations.find
        implementations
        { name = Menu.version_menu_rpc_name; version = 1 }
    with
    | Some { f = Legacy_menu_rpc menu; _ } ->
      (* 1. There exists a default menu rpc. We can dispatch it to retrieve the menu then
         include this in the metadata. menu rpc or a custom rpc to figure out whether we
         can dispatch it to retrieve the menu. *)
      Some (force menu), implementations
    | Some (_ : _ Implementation.t) ->
      (* 2. There was a custom menu rpc implemented. We cannot include a menu in the
         metadata. *)
      None, implementations
    | None ->
      (* 3. There is no menu so we are free to send a menu containing all of the
         implementations in the metadata. We should also include the menu rpc in the
         implementation so that we can eventually deprecate the custom implementation
         type. Old clients can't complain about the menu rpc being added because they
         don't have a way to figure out the list of available rpcs ;) *)
      let menu = Implementations.descriptions_and_shapes implementations in
      ( Some menu
      , Implementations.add_exn
          implementations
          { Implementation_types.Implementation.tag =
              Protocol.Rpc_tag.of_string Menu.version_menu_rpc_name
          ; version = 1
          ; f = Legacy_menu_rpc (lazy menu)
          ; shapes =
              lazy
                (Rpc_shapes.Rpc
                   { query = Menu.Stable.V1.bin_query.shape
                   ; response = Menu.Stable.V1.bin_response.shape
                   }
                 |> fun shapes -> shapes, Rpc_shapes.eval_to_digest shapes)
          ; on_exception =
              { callback = None; close_connection_if_no_return_value = false }
          } )
  in
  match%map do_handshake t writer ~handshake_timeout ~header ~menu ?identification () with
  | Ok negotiated_protocol_version ->
    run_after_handshake
      t
      ~negotiated_protocol_version
      ~implementations
      ~connection_state
      ~writer_monitor_exns;
    Ok t
  | Error error ->
    Error (Handshake_error.to_exn ~connection_description:description error)
;;

let with_close
  ?implementations
  ?handshake_timeout
  ?heartbeat_config
  ?description
  ?time_source
  ~connection_state
  transport
  ~dispatch_queries
  ~on_handshake_error
  =
  let handle_handshake_error =
    match on_handshake_error with
    | `Call f -> f
    | `Raise -> raise
  in
  let%bind t =
    create
      ?implementations
      ?handshake_timeout
      ?heartbeat_config
      ?description
      ?time_source
      ~connection_state
      transport
  in
  match t with
  | Error e ->
    let%bind () = Transport.close transport in
    handle_handshake_error e
  | Ok t ->
    Monitor.protect
      ~run:`Schedule
      ~rest:`Log
      ~finally:(fun () ->
        close t ~reason:(Info.of_string "Rpc.Connection.with_close finished"))
      (fun () ->
        let%bind result = dispatch_queries t in
        let%map () =
          match implementations with
          | None -> Deferred.unit
          | Some _ -> close_finished t
        in
        result)
;;

let server_with_close
  ?handshake_timeout
  ?heartbeat_config
  ?description
  ?time_source
  transport
  ~implementations
  ~connection_state
  ~on_handshake_error
  =
  let on_handshake_error =
    match on_handshake_error with
    | `Call f -> `Call f
    | `Raise -> `Raise
    | `Ignore -> `Call (fun _ -> Deferred.unit)
  in
  with_close
    ?handshake_timeout
    ?heartbeat_config
    ?description
    ?time_source
    transport
    ~implementations
    ~connection_state
    ~on_handshake_error
    ~dispatch_queries:(fun _ -> Deferred.unit)
;;

let close
  ?streaming_responses_flush_timeout
  ?(reason = Info.of_string "Rpc.Connection.close")
  t
  =
  close ?streaming_responses_flush_timeout ~reason t
;;

module Client_implementations = struct
  type conn = t

  type t =
    | T :
        { connection_state : conn -> 's
        ; implementations : 's Implementations.t
        }
        -> t

  let null () =
    T { connection_state = (fun _ -> ()); implementations = Implementations.null () }
  ;;
end

module For_testing = struct
  module Header = Header

  let with_async_execution_context ~context ~f =
    Async_kernel_scheduler.with_local handshake_header_override_key (Some context) ~f
  ;;
end
