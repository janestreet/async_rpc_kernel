open Core
open Async_kernel
module Time_ns = Core_private.Time_ns_alternate_sexp
module P = Protocol
module Reader = Transport.Reader
module Writer = Transport.Writer

module Peer_metadata = struct
  type t =
    | Unsupported
    | Expected of
        (Protocol.Connection_metadata.V2.t, [ `Connection_closed ]) Result.t Ivar.t
  [@@deriving sexp_of]
end

let heartbeat_timeout_override_env_var = "ASYNC_RPC_HEARTBEAT_TIMEOUT_OVERRIDE"

let heartbeat_timeout_override_from_environment =
  try
    Sys.getenv heartbeat_timeout_override_env_var |> Option.map ~f:Time_ns.Span.of_string
  with
  | exn ->
    Exn.reraise exn [%string "Failed to parse %{heartbeat_timeout_override_env_var}"]
;;

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

  let never_heartbeat =
    { timeout = Time_ns.Span.max_value_representable
    ; send_every = Time_ns.Span.min_value_representable
    }
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

(* We put Expert in the names of a few variants because these variants lead to ‘expert’
   tracing events which we document as only being caused by expert dispatches. If we
   change non-expert dispatches to use the variants we’ll need to change the doc or the
   code. *)
module Response_handler_action = struct
  type response_with_determinable_status =
    | Pipe_eof
    | Expert_indeterminate
    | Determinable :
        ('a Rpc_result.t[@globalized])
        * ('a Implementation_mode.Error_mode.t[@globalized])
        -> response_with_determinable_status

  type t =
    | Keep
    | Wait of (unit Deferred.t[@globalized])
    | Remove of (response_with_determinable_status, Rpc_error.t Modes.Global.t) result
    | Expert_remove_and_wait of (unit Deferred.t[@globalized])
end

module Response_handler = struct
  type t =
    data:Nat0.t Rpc_result.t
    -> read_buffer:Bigstring.t
    -> read_buffer_pos_ref:int ref
    -> Response_handler_action.t
end

module Close_reason = struct
  type t =
    | By_unknown of
        { send_reason_to_peer : bool
        ; reason : Info.t
        }
    | By_local of
        { send_reason_to_peer : bool
        ; reason : Info.t
        }
    | By_remote of Info.t

  let info_of_t = function
    | By_unknown { reason; send_reason_to_peer = _ } -> reason
    | By_local { reason; send_reason_to_peer = _ } ->
      Info.tag reason ~tag:"Connection closed by local side:"
    | By_remote reason -> Info.tag reason ~tag:"Connection closed by remote side:"
  ;;
end

type t =
  { description : Info.t
  ; heartbeat_config : Heartbeat_config.Runtime.t
  ; mutable heartbeat_callbacks : (unit -> unit) array
  ; mutable last_seen_alive : Time_ns.t
  ; max_metadata_size : Byte_units.t
  ; reader : Reader.t
  ; writer : Protocol_writer.t
  ; open_queries :
      (P.Query_id.t, Description.t * (Response_handler.t[@sexp.opaque])) Hashtbl.t
  ; close_started : Info.t Ivar.t
  ; close_finished : unit Ivar.t
      (* There's a circular dependency between connections and their implementation instances
     (the latter depends on the connection state, which is given access to the connection
     when it is created). *)
  ; implementations_instance : Implementations.Instance.t Set_once.t
  ; time_source : Synchronous_time_source.t
  ; heartbeat_event : Synchronous_time_source.Event.t Set_once.t
      (* Variant is decided once the protocol version negotiation is completed -- then, either
     sending the id is unsupported, or the id is requested and is on its way or received
      *)
  ; tracing_events : (Tracing_event.t -> unit) Bus.Read_write.t
  ; metadata_for_dispatch :
      (Description.t -> query_id:Int63.t -> Rpc_metadata.V1.t option) Moption.t
  ; peer_metadata : Peer_metadata.t Set_once.t
      (* responses to queries are written by the implementations instance. Other events
     are written by this module. *)
  ; metadata_on_receive_to_add_to_implementations_instance :
      (Description.t
       -> query_id:P.Query_id.t
       -> Rpc_metadata.V1.t option
       -> Execution_context.t
       -> Execution_context.t)
        Set_once.t
      (* this is used if [set_metadata_hooks] is called before we have filled
     [implementations_instance]. *)
  ; my_menu : Menu.t option
  ; received_close_reason : Info.t Set_once.t
  ; has_drained_reader : unit Ivar.t Set_once.t
  ; reader_drain_timeout : Time_ns.Span.t
  ; heartbeat_timeout_override : Time_ns.Span.t option
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
    | Ok metadata -> return metadata.identification
    | Error `Unsupported | Error `No_handshake | Error (`Closed (_ : Sexp.t lazy_t)) ->
      return None)
;;

let peer_menu t =
  map_metadata t ~kind_of_metadata:"peer_menu" ~f:(function
    | Ok metadata -> return (Ok metadata.menu)
    | Error `Unsupported | Error `No_handshake -> return (Ok None)
    | Error (`Closed info) -> return (Error (Error.of_lazy_sexp info)))
;;

let peer_menu' t =
  map_metadata t ~kind_of_metadata:"peer_menu" ~f:(function
    | Ok metadata -> return (Ok metadata.menu)
    | Error `Unsupported | Error `No_handshake -> return (Ok None)
    | Error (`Closed (_ : Sexp.t lazy_t)) -> return (Error Rpc_error.Connection_closed))
;;

let my_menu t = t.my_menu

let writer t =
  if is_closed t || not (Protocol_writer.can_send t.writer)
  then Error `Closed
  else Ok t.writer
;;

let bytes_to_write t = Protocol_writer.bytes_to_write t.writer
let bytes_written t = Protocol_writer.bytes_written t.writer
let bytes_read t = Reader.bytes_read t.reader
let flushed t = Protocol_writer.flushed t.writer
let tracing_events t = (t.tracing_events :> _ Bus.Read_only.t)
let have_metadata_hooks_been_set t = Moption.is_some t.metadata_for_dispatch

let set_metadata_hooks t ~when_sending ~on_receive =
  (* We only allow metadata hooks to be set once to simplify the possible misconfiguration
     issues with either overwriting or supporting multiple metadata hooks. *)
  if have_metadata_hooks_been_set t
  then `Already_set
  else (
    Moption.set_some t.metadata_for_dispatch when_sending;
    let on_receive =
      (on_receive
        : _ -> query_id:Int63.t -> _ -> _ -> _
        :> _ -> query_id:P.Query_id.t -> _ -> _ -> _)
    in
    (match Set_once.get t.implementations_instance with
     | Some instance -> Implementations.Instance.set_on_receive instance on_receive
     | None ->
       Set_once.set_exn
         t.metadata_on_receive_to_add_to_implementations_instance
         on_receive);
    `Ok)
;;

let write_tracing_event t event =
  if not (Bus.is_closed t.tracing_events) then Bus.write_local t.tracing_events event
;;

let compute_metadata t description query_id =
  if Moption.is_some t.metadata_for_dispatch
  then
    (Moption.unsafe_get t.metadata_for_dispatch)
      description
      ~query_id:(query_id : P.Query_id.t :> Int63.t)
  else None
;;

module Dispatch_error = struct
  type t =
    | Closed
    | Message_too_big of Transport.Send_result.message_too_big
  [@@deriving sexp_of]
end

let handle_send_result
  : 'a.
  t
  -> 'a Transport.Send_result.t
  -> rpc:Description.t
  -> kind:_ Tracing_event.Kind.t
  -> id:P.Query_id.t
  -> sending_one_way_rpc:bool
  -> ('a, Dispatch_error.t) Result.t
  =
  fun t r ~rpc ~kind ~id ~sending_one_way_rpc ->
  let id = (id :> Int63.t) in
  match r with
  | Sent { result = x; bytes } ->
    let ev : Tracing_event.t = { event = Sent kind; rpc; id; payload_bytes = bytes } in
    write_tracing_event t ev;
    if sending_one_way_rpc
    then
      write_tracing_event t { ev with event = Received (Response One_way_so_no_response) };
    Ok x
  | Closed ->
    write_tracing_event
      t
      { event = Failed_to_send (kind, Closed); rpc; id; payload_bytes = 0 };
    Error Dispatch_error.Closed
  | Message_too_big err ->
    write_tracing_event
      t
      { event = Failed_to_send (kind, Too_large); rpc; id; payload_bytes = err.size };
    Error
      (Dispatch_error.Message_too_big
         ([%globalize: Transport.Send_result.message_too_big] err))
;;

let send_query_with_registered_response_handler
  t
  (query : 'query P.Query.V2.t)
  ~response_handler
  ~kind
  ~(send_query : 'query P.Query.V2.t -> 'response Transport.Send_result.t)
  : ('response, Dispatch_error.t) Result.t
  =
  let rpc : Description.t =
    { name = P.Rpc_tag.to_string query.tag; version = query.version }
  in
  let registered_response_handler =
    match response_handler with
    | Some response_handler ->
      Hashtbl.set t.open_queries ~key:query.id ~data:(rpc, response_handler);
      true
    | None -> false
  in
  let result =
    send_query query
    |> handle_send_result
         t
         ~rpc
         ~kind
         ~id:query.id
         ~sending_one_way_rpc:(Option.is_none response_handler)
  in
  if registered_response_handler && Result.is_error result
  then Hashtbl.remove t.open_queries query.id;
  result
;;

let dispatch t ~kind ~response_handler ~bin_writer_query ~(query : _ P.Query.V2.t) =
  match writer t with
  | Error `Closed -> Error Dispatch_error.Closed
  | Ok writer ->
    let query =
      match query.metadata with
      | Some metadata ->
        { query with
          metadata =
            Some
              (Rpc_metadata.V1.truncate
                 metadata
                 (Byte_units.bytes_int_exn t.max_metadata_size))
        }
      | None -> query
    in
    send_query_with_registered_response_handler
      t
      query
      ~response_handler
      ~kind
      ~send_query:(fun query -> Protocol_writer.Query.send writer query ~bin_writer_query)
    [@nontail]
;;

let make_dispatch_bigstring
  do_send
  ~compute_metadata
  t
  ~tag
  ~version
  ~metadata
  buf
  ~pos
  ~len
  ~response_handler
  =
  match writer t with
  | Error `Closed -> Error Dispatch_error.Closed
  | Ok writer ->
    let id = P.Query_id.create () in
    let metadata = compute_metadata t ~tag ~version ~id ~metadata in
    let query : unit P.Query.V2.t = { tag; version; id; metadata; data = () } in
    send_query_with_registered_response_handler
      t
      query
      ~response_handler
      ~kind:Query
      ~send_query:(fun query ->
        Protocol_writer.Query.send_expert
          writer
          query
          ~buf
          ~pos
          ~len
          ~send_bin_prot_and_bigstring:do_send)
    [@nontail]
;;

let dispatch_bigstring =
  make_dispatch_bigstring
    Writer.send_bin_prot_and_bigstring
    ~compute_metadata:(fun t ~tag ~version ~id ~metadata:() ->
      compute_metadata t { name = P.Rpc_tag.to_string tag; version } id)
;;

let schedule_dispatch_bigstring =
  make_dispatch_bigstring
    Writer.send_bin_prot_and_bigstring_non_copying
    ~compute_metadata:(fun t ~tag ~version ~id ~metadata:() ->
      compute_metadata t { name = P.Rpc_tag.to_string tag; version } id)
;;

let schedule_dispatch_bigstring_with_metadata =
  make_dispatch_bigstring
    Writer.send_bin_prot_and_bigstring_non_copying
    ~compute_metadata:(fun _ ~tag:_ ~version:_ ~id:_ ~metadata -> metadata)
;;

let handle_response
  t
  id
  ~(data : Nat0.t Rpc_result.t)
  ~read_buffer
  ~read_buffer_pos_ref
  ~protocol_message_len
  : _ Transport.Handler_result.t
  =
  match Hashtbl.find t.open_queries id with
  | None -> Stop (Error (Rpc_error.Unknown_query_id id))
  | Some (rpc, response_handler) ->
    let payload_bytes =
      protocol_message_len
      +
      match data with
      | Ok x -> (x :> int)
      | Error _ -> 0
    in
    let response_event kind ~payload_bytes =
      write_tracing_event
        t
        { Tracing_event.event = Received (Response kind)
        ; rpc
        ; id :> Int63.t
        ; payload_bytes
        };
      ()
    in
    (match response_handler ~data ~read_buffer ~read_buffer_pos_ref with
     | Keep ->
       response_event Partial_response ~payload_bytes;
       Continue
     | Wait wait ->
       response_event Partial_response ~payload_bytes;
       Wait wait
     | Expert_remove_and_wait wait ->
       response_event
         (match data with
          | Ok _ -> Response_finished_expert_uninterpreted
          | Error err -> Response_finished_rpc_error_or_exn err)
         ~payload_bytes;
       Hashtbl.remove t.open_queries id;
       Wait wait
     | Remove removal_circumstances ->
       Hashtbl.remove t.open_queries id;
       (match removal_circumstances with
        | Ok (Determinable (result, error_mode)) ->
          if Bus.num_subscribers t.tracing_events > 0
          then (
            let kind : Tracing_event.Received_response_kind.t =
              match error_mode, result with
              | _, Error err -> Response_finished_rpc_error_or_exn err
              | Always_ok, Ok _ -> Response_finished_ok
              | Using_result, Ok (Ok _) -> Response_finished_ok
              | Using_result, Ok (Error _) -> Response_finished_user_defined_error
              | Using_result_result, Ok (Ok (Ok _)) -> Response_finished_ok
              | Using_result_result, Ok (Ok (Error _)) ->
                Response_finished_user_defined_error
              | Using_result_result, Ok (Error _) -> Response_finished_user_defined_error
              | Is_error is_error, Ok x ->
                (match is_error x with
                 | true -> Response_finished_user_defined_error
                 | false -> Response_finished_ok
                 | exception (err : exn) ->
                   Response_finished_rpc_error_or_exn (Uncaught_exn [%sexp (err : exn)]))
              | ( Streaming_initial_message
                , Ok { initial = Error _; unused_query_id = (_ : P.Unused_query_id.t) } )
                -> Response_finished_user_defined_error
              | ( Streaming_initial_message
                , Ok { initial = Ok _; unused_query_id = (_ : P.Unused_query_id.t) } ) ->
                (* This case should be impossible: we never remove after streaming begins *)
                Response_finished_ok
            in
            response_event kind ~payload_bytes);
          Continue
        | Ok Pipe_eof ->
          response_event
            (if Result.is_error data
             then Response_finished_rpc_error_or_exn Connection_closed
             else Response_finished_ok)
            ~payload_bytes;
          Continue
        | Ok Expert_indeterminate ->
          response_event
            (match data with
             | Ok _ -> Response_finished_expert_uninterpreted
             | Error err -> Response_finished_rpc_error_or_exn err)
            ~payload_bytes;
          Continue
        | Error { global = e } ->
          response_event (Response_finished_rpc_error_or_exn e) ~payload_bytes;
          (match e with
           | Unimplemented_rpc _ -> Continue
           | Bin_io_exn _
           | Connection_closed
           | Write_error _
           | Uncaught_exn _
           | Unknown_query_id _
           | Authorization_failure _
           | Message_too_big _
           | Unknown _
           | Lift_error _ -> Stop (Error e))))
;;

let set_peer_metadata t connection_metadata =
  match Set_once.get t.peer_metadata with
  | None | Some Unsupported ->
    raise_s
      [%message
        "Inconsistent state: receiving a metadata message is unsupported, but a metadata \
         message was received"]
  | Some (Expected ivar) ->
    if Ivar.is_empty t.close_started then Ivar.fill_exn ivar (Ok connection_metadata)
;;

let handle_msg
  t
  (msg : _ P.Message.t)
  ~read_buffer
  ~read_buffer_pos_ref
  ~protocol_message_len
  ~close_connection_monitor
  : _ Transport.Handler_result.t
  =
  match msg with
  | Metadata metadata ->
    set_peer_metadata t (P.Connection_metadata.V2.of_v1 metadata);
    Continue
  | Metadata_v2 metadata ->
    set_peer_metadata t metadata;
    Continue
  | Heartbeat ->
    Array.iter t.heartbeat_callbacks ~f:(fun f -> f ());
    Continue
  | Response_v1 response ->
    handle_response
      t
      response.id
      ~data:response.data
      ~read_buffer
      ~read_buffer_pos_ref
      ~protocol_message_len
  | Response_v2 response ->
    handle_response
      t
      response.id
      ~data:response.data
      ~read_buffer
      ~read_buffer_pos_ref
      ~protocol_message_len
  | Query_v2 query ->
    let instance = Set_once.get_exn t.implementations_instance in
    Implementations.Instance.handle_query
      instance
      ~query
      ~read_buffer
      ~read_buffer_pos_ref
      ~message_bytes_for_tracing:(protocol_message_len + (query.data :> int))
      ~close_connection_monitor
  | Query_v1 query ->
    let instance = Set_once.get_exn t.implementations_instance in
    let query = P.Query.V2.of_v1 query in
    Implementations.Instance.handle_query
      instance
      ~query
      ~read_buffer
      ~read_buffer_pos_ref
      ~message_bytes_for_tracing:(protocol_message_len + (query.data :> int))
      ~close_connection_monitor
  | Close_reason info | Close_reason_duplicated info ->
    Set_once.set_exn t.received_close_reason info;
    Stop (Ok ())
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

let close
  ?(streaming_responses_flush_timeout = Time_ns.Span.of_int_sec 5)
  ~(reason : Close_reason.t)
  t
  =
  if not (is_closed t)
  then (
    abort_heartbeating t;
    (* If our [received_close_reason] is set, it means our peer initiated the connection
         close, so we shouldn't bother sending our reason. *)
    if Set_once.is_none t.received_close_reason
    then (
      match reason with
      | By_local { send_reason_to_peer = true; reason }
      | By_unknown { send_reason_to_peer = true; reason } ->
        Result.iter (writer t) ~f:(fun writer ->
          let send_result =
            Protocol_writer.send_close_reason_if_supported writer ~reason
          in
          (* We should do nothing if we can't send the close reason. If the connection is
             closed, that's fine, we were trying to close the connection anyways. If the
             message is too big, the peer won't get the close reason, but it's not worth
             raising over. *)
          Option.iter__local send_result ~f:(function
            | Sent { result = (); bytes = (_ : int) }
            | Closed
            | Message_too_big (_ : Transport_intf.Send_result.message_too_big)
            -> ())
            [@nontail])
      | By_remote (_ : Info.t)
      | By_local { send_reason_to_peer = false; reason = (_ : Info.t) }
      | By_unknown { send_reason_to_peer = false; reason = (_ : Info.t) } -> ());
    Ivar.fill_exn
      t.close_started
      (Info.of_list
         [ Close_reason.info_of_t reason
         ; Info.tag t.description ~tag:"connection_description"
         ]);
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
        ~protocol_message_len:(!pos_ref - pos)
        ~close_connection_monitor
    with
    | Continue -> Continue
    | Wait _ as res -> res
    | Stop result ->
      let reason =
        let reason_if_no_error =
          match Set_once.get t.received_close_reason with
          | Some info ->
            (* Directly received close reason from peer. *)
            Close_reason.By_remote info
          | None ->
            Close_reason.By_unknown
              { reason = Info.of_string "Rpc message handling loop stopped"
              ; send_reason_to_peer = false
              }
        in
        match result with
        | Ok () -> reason_if_no_error
        | Error e ->
          Close_reason.By_unknown
            { reason =
                Info.of_list
                  [ reason_if_no_error |> Close_reason.info_of_t
                  ; Info.create_s
                      (Rpc_error.sexp_of_t_with_reason
                         e
                         ~get_connection_close_reason:(fun () ->
                           [%sexp
                             "Connection.on_message resulted in Connection_closed error. \
                              This is weird."]))
                  ]
            ; (* Similar to above, we recieved stop with an error which could come from the
                 local handler or we could have received an error over the wire. *)
              send_reason_to_peer = true
            }
      in
      don't_wait_for (close t ~reason);
      Stop reason
  in
  Staged.stage f
;;

let time_ns_to_microsecond_string time =
  Time_ns.round_down_to_us time
  |> Time_ns.to_string_abs_trimmed ~zone:(Lazy.force Timezone.local)
;;

let effective_heartbeat_timeout t =
  Option.value_map
    t.heartbeat_timeout_override
    ~default:t.heartbeat_config.timeout
    ~f:(fun heartbeat_timeout_override ->
      Time_ns.Span.max heartbeat_timeout_override t.heartbeat_config.timeout)
;;

let heartbeat_now t =
  let now = Synchronous_time_source.now t.time_source in
  let since_last_heartbeat = Time_ns.diff now t.last_seen_alive in
  let timeout = effective_heartbeat_timeout t in
  if Time_ns.Span.( > ) since_last_heartbeat timeout
  then (
    let reason () =
      [%string
        "No heartbeats received for %{timeout#Time_ns.Span}. Last seen at: \
         %{time_ns_to_microsecond_string t.last_seen_alive}, now: \
         %{time_ns_to_microsecond_string now}."]
    in
    (* Local didn't see heartbeats in time and decided to close.

       However, worth noting that just because we decided to close, it doesn't mean that
       it's definitely the local or remote that missed the heartbeat. This could've been
       caused by the remote failing to heartbeat, or the local being too busy to process
       any messages *)
    don't_wait_for
      (close
         t
         ~reason:
           (Close_reason.By_local
              { reason = Info.of_thunk reason; send_reason_to_peer = true })))
  else (
    match Protocol_writer.send_heartbeat t.writer with
    | Sent { result = (); bytes = (_ : int) } -> ()
    | Closed -> ()
    | Message_too_big _ as result ->
      let reason = [%globalize: unit Transport.Send_result.t] result in
      raise_s
        [%sexp
          "Heartbeat cannot be sent"
          , { reason : unit Transport.Send_result.t; connection = (t : t_hum_writer) }])
;;

let default_handshake_timeout = Time_ns.Span.of_int_sec 30
let default_reader_drain_timeout = Time_ns.Span.of_int_sec 5

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
    Hashtbl.iteri
      t.open_queries
      ~f:
        (fun
          ~key:(_ : Protocol.Query_id.t) ~data:((_ : Description.t), response_handler) ->
        ignore
          (response_handler
             ~read_buffer:dummy_buffer
             ~read_buffer_pos_ref:dummy_ref
             ~data:(Error error)));
    Hashtbl.clear t.open_queries;
    Bigstring.unsafe_destroy dummy_buffer)
;;

let schedule_heartbeats t =
  t.last_seen_alive <- Synchronous_time_source.now t.time_source;
  let event =
    Synchronous_time_source.Event.create t.time_source (fun () -> heartbeat_now t)
  in
  if Time_ns.Span.( >= ) t.heartbeat_config.send_every Time_ns.Span.zero
  then
    (* [at_intervals] will schedule the first heartbeat the first time the time_source is
       advanced *)
    Synchronous_time_source.Event.schedule_at_intervals
      t.time_source
      event
      t.heartbeat_config.send_every
    |> ok_exn;
  Set_once.set_exn t.heartbeat_event event
;;

let run_connection t ~implementations ~menu ~connection_state ~writer_monitor_exns =
  let instance =
    Implementations.instantiate
      implementations
      ~menu
      ~writer:t.writer
      ~tracing_events:t.tracing_events
      ~connection_description:t.description
      ~connection_close_started:(Ivar.read t.close_started)
      ~connection_state
  in
  (match Set_once.get t.metadata_on_receive_to_add_to_implementations_instance with
   | None -> ()
   | Some on_receive -> Implementations.Instance.set_on_receive instance on_receive);
  Set_once.set_exn t.implementations_instance instance;
  let has_drained_reader = Ivar.create () in
  (* Ensure that we set [has_drained_reader] as soon as we've completed the handshake so
     that if the connection dies after this, we make sure to wait and see if we got a
     close reason. *)
  Set_once.set_if_none t.has_drained_reader has_drained_reader;
  let close_connection_monitor = Monitor.create ~name:"RPC close connection monitor" () in
  Monitor.detach_and_iter_errors close_connection_monitor ~f:(fun exn ->
    (* An exception within the user provided implementation function shouldn't be affected
       by the remote *)
    let reason =
      Close_reason.By_local
        { reason =
            Info.create_s [%message "Uncaught exception in implementation" (exn : Exn.t)]
        ; send_reason_to_peer = true
        }
    in
    don't_wait_for (close ~reason t));
  let monitor = Monitor.create ~name:"RPC connection loop" () in
  (* It's not obvious where an exception in the RPC connection comes from so let's tag
     with [By_unknown] to be safe. It'd be a little surprising if we could raise based on
     something the peer did but it's hard to guarantee in the types. *)
  let reason name exn =
    ( exn
    , Close_reason.By_unknown
        { reason = Info.tag (Info.of_exn exn) ~tag:("exn raised in RPC connection " ^ name)
        ; send_reason_to_peer = true
        } )
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
    >>> fun result ->
    Ivar.fill_if_empty has_drained_reader ();
    match result with
    | Ok reason ->
      (* [on_message] only returns a reason when it gets a [Stop], which always sets the
         reason to be "Rpc message handling loop stopped" - that's not a reason we care
         about sending. *)
      cleanup t ~reason (Rpc_error.Rpc (Connection_closed, t.description))
    (* The protocol is such that right now, the only outcome of the other side closing the
       connection normally is that we get an eof. *)
    | Error (`Eof | `Closed) ->
      (* Seems like in most cases this is because the remote closed the connection without
         sending a stop message, but it's hard to verify that there are no other cases *)
      cleanup
        t
        ~reason:
          (Close_reason.By_unknown
             { reason = Info.of_string "EOF or connection closed"
             ; send_reason_to_peer = false
             })
        (Rpc_error.Rpc (Connection_closed, t.description)))
;;

let run_after_handshake t ~implementations ~menu ~connection_state ~writer_monitor_exns =
  let connection_state = connection_state t in
  if not (is_closed t)
  then (
    run_connection t ~implementations ~menu ~connection_state ~writer_monitor_exns;
    assert (Set_once.is_some t.heartbeat_event);
    assert (Set_once.is_some t.implementations_instance);
    if is_closed t then abort_heartbeating t)
;;

let negotiate ?identification t ~header ~peer ~menu =
  let%bind.Result version = Header.negotiate ~us:header ~peer in
  Protocol_writer.set_negotiated_protocol_version t.writer version;
  if Version_dependent_feature.(is_supported Peer_metadata ~version)
  then (
    let ivar = Ivar.create () in
    upon (Ivar.read t.close_started) (fun (_ : Info.t) ->
      Ivar.fill_if_empty ivar (Error `Connection_closed));
    Set_once.set_exn t.peer_metadata (Expected ivar);
    Protocol_writer.For_handshake.send_connection_metadata_if_supported
      t.writer
      menu
      ~identification)
  else (
    Set_once.set_exn t.peer_metadata Unsupported;
    Ok ())
;;

let do_handshake ?identification t ~handshake_timeout ~header ~menu =
  match Protocol_writer.For_handshake.send_handshake_header t.writer header with
  | Error error -> return (Error error)
  | Ok () ->
    (* If we use [max_connections] in the server, then this read may just hang until the
       server starts accepting new connections (which could be never).  That is why a
       timeout is used *)
    let result =
      Monitor.try_with_local ~rest:`Log (fun () ->
        Reader.read_one_message_bin_prot t.reader Header.bin_t.reader)
    in
    let handshake_started_at = Synchronous_time_source.now t.time_source in
    (match%bind
       Time_source.with_timeout
         (Time_source.of_synchronous t.time_source)
         handshake_timeout
         result
     with
     | `Timeout ->
       let now = Synchronous_time_source.now t.time_source in
       let reason () =
         [%string
           "Handshake timeout after %{handshake_timeout#Time_ns.Span}. Handshake started \
            at %{time_ns_to_microsecond_string handshake_started_at}, now: \
            %{time_ns_to_microsecond_string now}."]
       in
       (* There's a pending read, the reader is basically useless now, so we clean it
         up. *)
       don't_wait_for
         (close
            t
            ~reason:
              ((* A handshake timeout could be a result of both the local and remote but
                  we're detecting it on the local side. This is similar to a heartbeat
                  timeout close reason. *)
                 Close_reason.By_local
                 { reason = Info.of_thunk reason; send_reason_to_peer = true }));
       return (Error Handshake_error.Timeout)
     | `Result (Error exn) ->
       let reason = Info.of_string "[Reader.read_one_message_bin_prot] raised" in
       don't_wait_for
         (close
            t
            ~reason:
              ((* Closed on local side since we failed to read the header. *)
                 Close_reason.By_local
                 { reason = Info.of_list [ reason; Info.of_exn exn ]
                 ; send_reason_to_peer = true
                 }));
       return (Error (Handshake_error.Reading_header_failed (Error.of_exn exn)))
     | `Result (Ok (Error `Eof)) -> return (Error Handshake_error.Eof)
     | `Result (Ok (Error `Closed)) -> return (Error Handshake_error.Transport_closed)
     | `Result (Ok (Ok peer)) -> return (negotiate ?identification t ~peer ~header ~menu))
;;

let contains_magic_prefix = Protocol_version_header.contains_magic_prefix ~protocol:Rpc
let default_handshake_header = Header.latest

let handshake_header_override_key =
  Univ_map.Key.create ~name:"async rpc handshake header override" [%sexp_of: Header.t]
;;

let get_handshake_header () =
  Async_kernel_scheduler.find_local handshake_header_override_key
  |> Option.value ~default:default_handshake_header
;;

let drain_reader_for_close_reason t ~default_close_reason =
  let%map (`Timeout | `Result ()) =
    match Set_once.get t.has_drained_reader with
    | None ->
      (* If we haven't even started the reader loop yet when we close then we'll never get
         a close reason. *)
      return (`Result ())
    | Some has_drained_reader ->
      Time_source.with_timeout
        (Time_source.of_synchronous t.time_source)
        t.reader_drain_timeout
        (Ivar.read has_drained_reader)
  in
  Set_once.get t.received_close_reason
  (* We received a close reason by the remote *)
  |> Option.map ~f:(fun reason -> Close_reason.By_remote reason)
  |> Option.value_or_thunk ~default:default_close_reason
;;

let create
  ?implementations
  ?protocol_version_headers
  ~connection_state
  ?(handshake_timeout = default_handshake_timeout)
  ?(heartbeat_config = Heartbeat_config.create ())
  ?(max_metadata_size = Byte_units.of_kilobytes 1.)
  ?(description = Info.of_string "<created-directly>")
  ?(time_source = Synchronous_time_source.wall_clock ())
  ?identification
  ?(reader_drain_timeout = default_reader_drain_timeout)
  ?(provide_rpc_shapes = false)
  ({ reader; writer } : Transport.t)
  =
  let implementations =
    match implementations with
    | None -> Implementations.null ()
    | Some s -> s
  in
  let menu, implementations =
    match
      Implementations.find
        implementations
        { name = Menu.version_menu_rpc_name; version = 1 }
    with
    | Some (_ : _ Implementation.t) ->
      (* 1. There was a custom menu rpc implemented. We cannot include a menu in the
         metadata. *)
      None, implementations
    | None ->
      (* 2. There is no menu so we are free to send a menu containing all of the
         implementations in the metadata. We should also include the menu rpc in the
         implementation so that we can eventually deprecate the custom implementation
         type. Old clients can't complain about the menu rpc being added because they
         don't have a way to figure out the list of available rpcs ;) *)
      let menu =
        if provide_rpc_shapes
        then
          Implementations.descriptions_and_shapes implementations
          |> Menu.of_supported_rpcs_and_shapes
        else
          Implementations.descriptions implementations
          |> Menu.of_supported_rpcs ~rpc_shapes:`Unknown
      in
      ( Some menu
      , Implementations.add_exn
          implementations
          { Implementation.tag = Protocol.Rpc_tag.of_string Menu.version_menu_rpc_name
          ; version = 1
          ; f = Menu_rpc (lazy menu)
          ; shapes =
              lazy
                (Rpc_shapes.Rpc
                   { query = Menu.Stable.V1.bin_query.shape
                   ; response = Menu.Stable.V1.bin_response.shape
                   }
                 |> fun shapes -> shapes, Rpc_shapes.eval_to_digest shapes)
          ; on_exception = Some Log_on_background_exn
          } )
  in
  let tracing_events =
    Bus.create_exn
      Arity1_local
      ~on_subscription_after_first_write:Allow
      ~on_callback_raise:Error.raise
  in
  let t =
    { description
    ; heartbeat_config = Heartbeat_config.to_runtime heartbeat_config
    ; heartbeat_callbacks = [||]
    ; last_seen_alive = Synchronous_time_source.now time_source
    ; max_metadata_size
    ; reader
    ; writer = Protocol_writer.create_before_negotiation writer ~tracing_events
    ; open_queries = Hashtbl.Poly.create ~size:10 ()
    ; close_started = Ivar.create ()
    ; close_finished = Ivar.create ()
    ; implementations_instance = Set_once.create ()
    ; time_source
    ; heartbeat_event = Set_once.create ()
    ; tracing_events
    ; metadata_for_dispatch = Moption.create ()
    ; peer_metadata = Set_once.create ()
    ; metadata_on_receive_to_add_to_implementations_instance = Set_once.create ()
    ; my_menu = menu
    ; received_close_reason = Set_once.create ()
    ; has_drained_reader = Set_once.create ()
    ; reader_drain_timeout
    ; heartbeat_timeout_override =
        heartbeat_timeout_override_from_environment
        (* The benefit of holding the override in [t] is that if the user programmatically
     increases the heartbeat timeout via [reset_heartbeat_timeout] and this exceeds both
     the previous heartbeat timeout and the environment override, then we'll use the new
     value *)
    }
  in
  let writer_monitor_exns = Monitor.detach_and_get_error_stream (Writer.monitor writer) in
  upon (Writer.stopped writer) (fun () ->
    don't_wait_for
      ((* Transport layer stopped, unclear if it was because of the local or remote side
       *)
       let%bind reason =
         drain_reader_for_close_reason t ~default_close_reason:(fun () ->
           Close_reason.By_unknown
             { reason = Info.of_string "RPC transport stopped"
             ; send_reason_to_peer = false
             })
       in
       close t ~reason));
  upon (Ivar.read t.close_finished) (fun () -> Bus.close t.tracing_events);
  let header = get_handshake_header () in
  let handshake_result =
    match protocol_version_headers with
    | None -> do_handshake ?identification t ~handshake_timeout ~header ~menu
    | Some { Protocol_version_header.Pair.us = header; peer } ->
      return (negotiate ?identification t ~header ~peer ~menu)
  in
  match%map handshake_result with
  | Ok () ->
    run_after_handshake t ~implementations ~menu ~connection_state ~writer_monitor_exns;
    Ok t
  | Error error ->
    Error (Handshake_error.to_exn ~connection_description:description error)
;;

let with_close
  ?implementations
  ?protocol_version_headers
  ?handshake_timeout
  ?heartbeat_config
  ?description
  ?time_source
  ?identification
  ?provide_rpc_shapes
  ~connection_state
  transport
  ~dispatch_queries
  ~on_handshake_error
  =
  let handle_handshake_error =
    match on_handshake_error with
    | `Call f -> f
    | `Raise -> fun x -> raise x
  in
  let%bind t =
    create
      ?implementations
      ?protocol_version_headers
      ?handshake_timeout
      ?heartbeat_config
      ?description
      ?time_source
      ?identification
      ?provide_rpc_shapes
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
        close
          t
          ~reason:
            ((* This indicates that the lambda below exited (or raised), which is on the
                local side *)
               Close_reason.By_local
               { reason = Info.of_string "Rpc.Connection.with_close finished"
               ; send_reason_to_peer = false
               }))
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
  ?identification
  ?provide_rpc_shapes
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
    ?identification
    ?provide_rpc_shapes
    transport
    ~implementations
    ~connection_state
    ~on_handshake_error
    ~dispatch_queries:(fun _ -> Deferred.unit)
;;

let close ?streaming_responses_flush_timeout ?reason t =
  (* User directly called close on the connection so this is definitely [By_local]. *)
  let reason =
    match reason with
    | None ->
      Close_reason.By_local
        { reason = Info.of_string "Rpc.Connection.close"; send_reason_to_peer = false }
    | Some reason -> Close_reason.By_local { reason; send_reason_to_peer = true }
  in
  close ?streaming_responses_flush_timeout t ~reason
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

  let heartbeat_timeout_override_env_var = heartbeat_timeout_override_env_var
end
