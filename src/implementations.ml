open Core
open Poly
open Async_kernel
open Util
open Implementation_types.Implementations
module P = Protocol

(* The Result monad is also used. *)
let ( >>|~ ) = Result.( >>| )

(* Commute Result and Deferred. *)
let defer_result : 'a 'b. ('a Deferred.t, 'b) Result.t -> ('a, 'b) Result.t Deferred.t
  = function
  | Error _ as err -> return err
  | Ok d ->
    (match Deferred.peek d with
     | None ->
       let%map x = d in
       Ok x
     | Some d -> return (Ok d))
;;

module Responder = Implementation.Expert.Responder

type 'connection_state on_unknown_rpc =
  [ `Raise
  | `Continue
  | `Close_connection
  | `Call of
    'connection_state
    -> rpc_tag:string
    -> version:int
    -> [ `Close_connection | `Continue ]
  ]

type 'connection_state on_unknown_rpc_with_expert =
  [ 'connection_state on_unknown_rpc
  | `Expert of
    'connection_state
    -> rpc_tag:string
    -> version:int
    -> metadata:string option
    -> Responder.t
    -> Bigstring.t
    -> pos:int
    -> len:int
    -> unit Deferred.t
  ]

type 'connection_state t = 'connection_state Implementation_types.Implementations.t =
  { implementations : 'connection_state Implementation.t Description.Table.t
  ; on_unknown_rpc : 'connection_state on_unknown_rpc_with_expert
  }

type 'connection_state implementations = 'connection_state t

let descriptions t = Hashtbl.keys t.implementations

let descriptions_and_shapes ?exclude_name t =
  Hashtbl.fold t.implementations ~init:[] ~f:(fun ~key ~data acc ->
    match exclude_name with
    | Some name when String.equal name key.name -> acc
    | _ -> (key, Implementation.digests data) :: acc)
;;

module Instance = struct
  type streaming_response = Instance.streaming_response =
    | Pipe : _ Pipe.Reader.t -> streaming_response
    | Direct :
        (_ Implementation_types.Direct_stream_writer.t[@sexp.opaque])
        -> streaming_response
  [@@deriving sexp_of]

  type streaming_responses = (P.Query_id.t, streaming_response) Hashtbl.t
  [@@deriving sexp_of]

  type 'a unpacked = 'a Instance.unpacked =
    { implementations : ('a implementations[@sexp.opaque])
    ; writer : Protocol_writer.t
    ; events : (Tracing_event.t -> unit) Bus.Read_write.t
    ; open_streaming_responses : streaming_responses
    ; mutable stopped : bool
    ; connection_state : 'a
    ; connection_description : Info.t
    ; connection_close_started : Info.t Deferred.t
    ; mutable
        last_dispatched_implementation :
        (Description.t * ('a Implementation.t[@sexp.opaque])) option
    ; mutable
        on_receive :
        Description.t
        -> query_id:P.Query_id.t
        -> Rpc_metadata.t option
        -> Execution_context.t
        -> Execution_context.t
    }
  [@@deriving sexp_of]

  type t = Instance.t = T : _ unpacked -> t [@@unboxed]

  let set_on_receive (T t) on_receive = t.on_receive <- on_receive
  let sexp_of_t (T t) = [%sexp_of: _ unpacked] t

  let send_write_error t id sexp =
    match
      Protocol_writer.send_response
        t.writer
        { id; data = Error (Write_error sexp) }
        ~bin_writer_response:Nothing.bin_writer_t
    with
    | Sent { result = (); bytes = _ } | Closed -> ()
    | Message_too_big _ as r ->
      raise_s
        [%sexp
          "Failed to send write error to client"
          , { error = (sexp : Sexp.t)
            ; reason =
                ([%globalize: unit Transport.Send_result.t] r
                  : unit Transport.Send_result.t)
            }]
  ;;

  let write_event t event =
    if not (Bus.is_closed t.events) then Bus.write_local t.events event
  ;;

  let handle_send_result t qid rpc kind (result : _ Transport.Send_result.t) =
    let id = (qid : P.Query_id.t :> Int63.t) in
    (match result with
     | Sent { result = _; bytes } ->
       write_event
         t
         { event = Sent (Response kind); rpc = Some rpc; id; payload_bytes = bytes }
     | Closed ->
       write_event
         t
         { event = Failed_to_send (Response kind, Closed)
         ; rpc = Some rpc
         ; id
         ; payload_bytes = 0
         }
     | Message_too_big err as r ->
       write_event
         t
         { event = Failed_to_send (Response kind, Too_large)
         ; rpc = Some rpc
         ; id
         ; payload_bytes = err.size
         };
       send_write_error
         t
         qid
         ([%sexp_of: unit Transport.Send_result.t]
            ([%globalize: unit Transport.Send_result.t] r)));
    ()
  ;;

  let unsafe_write_message_for_cached_bin_writer t bin_writer x ~id ~rpc ~kind =
    if not t.stopped
    then
      Protocol_writer.Unsafe_for_cached_bin_writer.send_bin_prot t.writer bin_writer x
      |> handle_send_result t id rpc kind;
    ()
  ;;

  let write_response'
    (type a)
    t
    id
    bin_writer_data
    (data : a Rpc_result.t)
    ~rpc
    ~(error_mode : a Implementation.F.error_mode)
    ~ok_kind
    =
    if not t.stopped
    then (
      let kind : Tracing_event.Sent_response_kind.t =
        (* We skip trying to determine if the response is an error if there are no
           subscribers as calling arbitrary functions may be expensive. *)
        if Bus.num_subscribers t.events = 0
        then ok_kind
        else (
          match error_mode, data with
          | _, Error _ -> Single_or_streaming_rpc_error_or_exn
          | Always_ok, Ok _ -> ok_kind
          | Using_result, Ok (Ok _) -> ok_kind
          | Using_result, Ok (Error _) -> Single_or_streaming_user_defined_error
          | Using_result_result, Ok (Ok (Ok _)) -> ok_kind
          | Using_result_result, Ok (Ok (Error _)) ->
            Single_or_streaming_user_defined_error
          | Using_result_result, Ok (Error _) -> Single_or_streaming_user_defined_error
          | Is_error is_error, Ok x ->
            (match is_error x with
             | true -> Single_or_streaming_user_defined_error
             | false -> ok_kind
             | exception (_ : exn) -> Single_or_streaming_rpc_error_or_exn)
          | ( Streaming_initial_message
            , Ok { initial = Ok _; unused_query_id = (_ : P.Unused_query_id.t) } ) ->
            ok_kind
          | ( Streaming_initial_message
            , Ok { initial = Error _; unused_query_id = (_ : P.Unused_query_id.t) } ) ->
            Single_or_streaming_user_defined_error)
      in
      (Protocol_writer.send_response
         t.writer
         { id; data }
         ~bin_writer_response:bin_writer_data
       |> handle_send_result t id rpc kind) [@nontail])
  ;;

  let write_streaming_eof t id bin_writer_data data ~rpc =
    write_response'
      t
      id
      bin_writer_data
      data
      ~rpc
      ~error_mode:Always_ok
      ~ok_kind:Streaming_closed
  ;;

  let write_streaming_initial t id bin_writer_data data is_final ~rpc ~error_mode =
    let ok_kind : Tracing_event.Sent_response_kind.t =
      match is_final with
      | `Final -> Single_or_streaming_rpc_error_or_exn
      | `Will_stream -> Streaming_initial
    in
    write_response' t id bin_writer_data data ~rpc ~error_mode ~ok_kind
  ;;

  let write_single_response t id bin_writer_data data ~rpc ~error_mode =
    write_response' t id bin_writer_data data ~rpc ~error_mode ~ok_kind:Single_succeeded
  ;;

  module Cached_bin_writer : sig
    type 'a t = 'a Implementation_types.Cached_bin_writer.t

    val create : id:P.Query_id.t -> bin_writer:'a Bin_prot.Type_class.writer -> 'a t
    val prep_write : 'a t -> 'a -> ('a t * 'a) Bin_prot.Type_class.writer
    val prep_write_expert : 'a t -> len:int -> 'a t Bin_prot.Type_class.writer
    val prep_write_string : 'a t -> string -> ('a t * string) Bin_prot.Type_class.writer
  end = struct
    type 'a t = 'a Implementation_types.Cached_bin_writer.t =
      { header_prefix : string (* Bin_protted constant prefix of the message *)
      ; (* Length of the user data part. We set this field when sending a message. This
           relies on the fact that the message is serialized immediately (which is the
           only acceptable semantics for the transport layer anyway, as it doesn't know if
           the value is mutable or not).

           [data_len] is passed to bin-prot writers by mutating [data_len] instead of by
           passing an additional argument to avoid some allocation.
        *)
        mutable data_len : Nat0.t
      ; bin_writer : 'a Bin_prot.Type_class.writer
      }

    type void = Void

    let bin_size_void Void = 0
    let bin_write_void _buf ~pos Void = pos

    type void_message = void P.Message.maybe_needs_length [@@deriving bin_write]

    type void_stream_response_data = void P.Stream_response_data.needs_length
    [@@deriving bin_write]

    (* This is not re-entrant but Async code always runs on one thread at a time *)
    let buffer = Bigstring.create 32

    let cache_bin_protted (bin_writer : _ Bin_prot.Type_class.writer) x =
      let len = bin_writer.write buffer ~pos:0 x in
      Bigstring.To_string.sub buffer ~pos:0 ~len
    ;;

    let create (type a) ~id ~bin_writer : a t =
      let header_prefix =
        cache_bin_protted bin_writer_void_message (Response { id; data = Ok Void })
      in
      { header_prefix; bin_writer; data_len = Nat0.of_int_exn 0 }
    ;;

    (* This part of the message header is a constant, make it a literal to make the
       writing code slightly faster. *)
    let stream_response_data_header_len = 4
    let stream_response_data_header_as_int32 = 0x8a79l

    let%test_unit "stream_response_* constants are correct" =
      let len =
        bin_writer_void_stream_response_data.write
          buffer
          ~pos:0
          (`Ok Void : void_stream_response_data)
      in
      assert (len = stream_response_data_header_len);
      assert (
        Bigstring.unsafe_get_int32_t_le buffer ~pos:0
        = stream_response_data_header_as_int32)
    ;;

    let bin_write_string_no_length buf ~pos str =
      let str_len = String.length str in
      (* Very low-level bin_prot stuff... *)
      Bin_prot.Common.assert_pos pos;
      let next = pos + str_len in
      Bin_prot.Common.check_next buf next;
      Bin_prot.Common.unsafe_blit_string_buf ~src_pos:0 str ~dst_pos:pos buf ~len:str_len;
      next
    ;;

    (* The two following functions are used by the 3 variants exposed by this module. They
       serialize a [Response { id; data = Ok (`Ok data_len) }] value, taking care of
       writing the [Nat0.t] length prefix where appropriate.

       Bear in mind that there are two levels of length prefixes for stream response data
       message: one for the user data (under the `Ok, before the actual data), and one for
       the response data (under the .data field, before the Ok).

       When eventually serialized, we get a sequence of bytes that can be broken down as:
       {v
        <Response><id> <length-of-rest> <Ok `Ok> <length-of-rest> <actual-response>
       |              |                |        |                |<- t.data_len -->|
       |header_prefix |             -->|        |<--             |                 |
       |                       stream_response_data_header_len   |                 |
       |                               |<------ stream_response_data_len --------->|
       |<----------------------- bin_size_nat0_header ---------------------------->|
       |<----------- written by bin_write_nat0_header ---------->|
       v}
    *)
    let bin_size_nat0_header { header_prefix; data_len; _ } =
      let stream_response_data_nat0_len =
        stream_response_data_header_len + Nat0.bin_size_t data_len
      in
      let stream_response_data_len =
        stream_response_data_nat0_len + (data_len : Nat0.t :> int)
      in
      String.length header_prefix
      + Nat0.bin_size_t (Nat0.of_int_exn stream_response_data_len)
      + stream_response_data_nat0_len
    ;;

    let bin_write_nat0_header buf ~pos { header_prefix; data_len; _ } =
      let pos = bin_write_string_no_length buf ~pos header_prefix in
      let stream_response_data_len =
        stream_response_data_header_len
        + Nat0.bin_size_t data_len
        + (data_len : Nat0.t :> int)
      in
      let pos = Nat0.bin_write_t buf ~pos (Nat0.of_int_exn stream_response_data_len) in
      let next = pos + 4 in
      Bin_prot.Common.check_next buf next;
      Bigstring.unsafe_set_int32_t_le buf ~pos stream_response_data_header_as_int32;
      Nat0.bin_write_t buf ~pos:next data_len
    ;;

    let bin_writer_nat0_header : _ Bin_prot.Type_class.writer =
      { size = bin_size_nat0_header; write = bin_write_nat0_header }
    ;;

    let bin_size_message (t, _) = bin_size_nat0_header t + (t.data_len : Nat0.t :> int)

    let bin_write_message buf ~pos (t, data) =
      let pos = bin_write_nat0_header buf ~pos t in
      t.bin_writer.write buf ~pos data
    ;;

    let bin_writer_message : _ Bin_prot.Type_class.writer =
      { size = bin_size_message; write = bin_write_message }
    ;;

    let bin_size_message_as_string (t, _) =
      bin_size_nat0_header t + (t.data_len : Nat0.t :> int)
    ;;

    let bin_write_message_as_string buf ~pos (t, str) =
      let pos = bin_write_nat0_header buf ~pos t in
      bin_write_string_no_length buf ~pos str
    ;;

    let bin_writer_message_as_string : _ Bin_prot.Type_class.writer =
      { size = bin_size_message_as_string; write = bin_write_message_as_string }
    ;;

    let prep_write t data =
      t.data_len <- Nat0.of_int_exn (t.bin_writer.size data);
      bin_writer_message
    ;;

    let prep_write_string t str =
      t.data_len <- Nat0.of_int_exn (String.length str);
      bin_writer_message_as_string
    ;;

    let prep_write_expert t ~len =
      t.data_len <- Nat0.of_int_exn len;
      bin_writer_nat0_header
    ;;
  end

  module Direct_stream_writer = struct
    module T = Implementation_types.Direct_stream_writer
    module State = T.State
    module Id = T.Id

    type 'a t = 'a T.t =
      { id : Id.t
      ; mutable state : 'a State.t
      ; closed : unit Ivar.t
      ; instance : Instance.t
      ; query_id : P.Query_id.t
      ; rpc : Description.t
      ; stream_writer : 'a Cached_bin_writer.t
      ; groups : 'a group_entry Bag.t
      }

    and 'a group_entry = 'a T.group_entry =
      { group : 'a T.Group.t
      ; element_in_group : 'a t Bag.Elt.t
      }

    let is_closed t = Ivar.is_full t.closed
    let closed t = Ivar.read t.closed

    let flushed t =
      let (T instance) = t.instance in
      Protocol_writer.flushed instance.writer
    ;;

    let bin_writer t = t.stream_writer.bin_writer

    let write_eof { instance = T instance; query_id; rpc; _ } =
      write_streaming_eof
        instance
        query_id
        P.Stream_response_data.bin_writer_nat0_t
        (Ok `Eof)
        ~rpc
    ;;

    (* [write_message_string] and [write_message] both allocate 3 words for the tuples.
       [write_message_expert] does not allocate. *)

    let write_message_string t x =
      let bin_writer_message_as_string =
        Cached_bin_writer.prep_write_string t.stream_writer x
      in
      let (T instance) = t.instance in
      unsafe_write_message_for_cached_bin_writer
        instance
        ~id:t.query_id
        ~rpc:t.rpc
        ~kind:Streaming_update
        bin_writer_message_as_string
        (t.stream_writer, x)
    ;;

    let write_message t x =
      let bin_writer_message = Cached_bin_writer.prep_write t.stream_writer x in
      let (T instance) = t.instance in
      unsafe_write_message_for_cached_bin_writer
        instance
        ~id:t.query_id
        ~rpc:t.rpc
        ~kind:Streaming_update
        bin_writer_message
        (t.stream_writer, x)
    ;;

    let write_message_expert t ~buf ~pos ~len =
      let bin_writer_message = Cached_bin_writer.prep_write_expert t.stream_writer ~len in
      let (T instance) = t.instance in
      if not instance.stopped
      then
        Protocol_writer.Unsafe_for_cached_bin_writer.send_bin_prot_and_bigstring
          instance.writer
          bin_writer_message
          t.stream_writer
          ~buf
          ~pos
          ~len
        |> handle_send_result instance t.query_id t.rpc Streaming_update;
      ()
    ;;

    let schedule_write_message_expert t ~buf ~pos ~len =
      let bin_writer_message = Cached_bin_writer.prep_write_expert t.stream_writer ~len in
      let (T instance) = t.instance in
      if not instance.stopped
      then (
        let result =
          Protocol_writer.Unsafe_for_cached_bin_writer
          .send_bin_prot_and_bigstring_non_copying
            instance.writer
            bin_writer_message
            t.stream_writer
            ~buf
            ~pos
            ~len
        in
        handle_send_result instance t.query_id t.rpc Streaming_update result;
        match result with
        | Sent { result; bytes = _ } -> `Flushed { Gel.g = result }
        | Closed -> `Closed
        | Message_too_big too_big ->
          failwiths
            "could not send as message too big"
            ([%globalize: Transport_intf.Send_result.message_too_big] too_big)
            [%sexp_of: Transport_intf.Send_result.message_too_big]
            ~here:[%here])
      else `Closed
    ;;

    let close_without_removing_from_instance t =
      if not (Ivar.is_full t.closed)
      then (
        Ivar.fill_exn t.closed ();
        let groups = t.groups in
        if not (Bag.is_empty groups)
        then
          Async_kernel_scheduler.Private.Very_low_priority_work.enqueue ~f:(fun () ->
            match Bag.remove_one groups with
            | None -> Finished
            | Some { group; element_in_group } ->
              Bag.remove group.components element_in_group;
              Hashtbl.remove group.components_by_id t.id;
              Not_finished);
        match t.state with
        | Not_started q ->
          Queue.iter q ~f:(function
            | Normal _ | Expert_string (_ : string) -> ()
            | Expert_schedule_bigstring
                { buf = (_ : Bigstring.t); pos = (_ : int); len = (_ : int); done_ } ->
              Ivar.fill_if_empty done_ ())
        | Started -> write_eof t)
    ;;

    let close ({ instance = T instance; query_id; _ } as t) =
      close_without_removing_from_instance t;
      Hashtbl.remove instance.open_streaming_responses query_id
    ;;

    let write_without_pushback t x =
      if Ivar.is_full t.closed
      then `Closed
      else (
        (match t.state with
         | Not_started q -> Queue.enqueue q (Normal x)
         | Started -> write_message t x);
        `Ok)
    ;;

    let write ({ instance = T instance; _ } as t) x =
      match write_without_pushback t x with
      | `Closed -> `Closed
      | `Ok -> `Flushed (Protocol_writer.flushed instance.writer)
    ;;

    module Expert = struct
      let write_without_pushback t ~buf ~pos ~len =
        if Ivar.is_full t.closed
        then `Closed
        else (
          (match t.state with
           | Not_started q ->
             (* we need to copy as the caller is allowed to mutate [buf] after this call *)
             Queue.enqueue q (Expert_string (Bigstring.To_string.sub buf ~pos ~len))
           | Started -> write_message_expert t ~buf ~pos ~len);
          `Ok)
      ;;

      let write ({ instance = T instance; _ } as t) ~buf ~pos ~len =
        match write_without_pushback t ~buf ~pos ~len with
        | `Closed -> `Closed
        | `Ok -> `Flushed (Protocol_writer.flushed instance.writer)
      ;;

      let schedule_write t ~buf ~pos ~len =
        if Ivar.is_full t.closed
        then `Closed
        else (
          match t.state with
          | Started -> schedule_write_message_expert t ~buf ~pos ~len
          | Not_started q ->
            let done_ = Ivar.create () in
            Queue.enqueue q (Expert_schedule_bigstring { buf; pos; len; done_ });
            `Flushed { g = Ivar.read done_ })
      ;;
    end

    let start t =
      match t.state with
      | Started -> failwith "attempted to start writer which was already started"
      | Not_started q ->
        t.state <- Started;
        Queue.iter q ~f:(function
          | Normal x -> write_message t x
          | Expert_string x -> write_message_string t x
          | Expert_schedule_bigstring { buf; pos; len; done_ } ->
            (match schedule_write_message_expert t ~buf ~pos ~len with
             | `Flushed { g = d } -> Eager_deferred.upon d (Ivar.fill_exn done_)
             | `Closed -> Ivar.fill_exn done_ ()));
        if Ivar.is_full t.closed then write_eof t
    ;;
  end

  let maybe_dispatch_on_exception
    (error : Rpc_error.t)
    on_exception
    ~close_connection_monitor
    =
    match error with
    | Uncaught_exn sexp ->
      On_exception.handle_exn on_exception ~close_connection_monitor (Exn.create_s sexp)
    | (_ : Rpc_error.t) -> ()
  ;;

  let authorization_failure_result error ~rpc_kind =
    let exn = Error.to_exn error in
    Rpc_result.authorization_error
      exn
      ~location:[%string "server-side %{rpc_kind} authorization"]
  ;;

  let apply_streaming_implementation
    t
    { Implementation.F.bin_query_reader
    ; bin_init_writer
    ; bin_update_writer
    ; impl
    ; error_mode
    }
    ~len
    ~read_buffer
    ~read_buffer_pos_ref
    ~id
    ~rpc
    ~(on_exception : On_exception.t)
    ~close_connection_monitor
    =
    let data =
      bin_read_from_bigstring
        bin_query_reader
        read_buffer
        ~pos_ref:read_buffer_pos_ref
        ~len
        ~location:"streaming_rpc server-side query un-bin-io'ing"
    in
    let stream_writer = Cached_bin_writer.create ~id ~bin_writer:bin_update_writer in
    let impl_with_state =
      match impl with
      | Pipe f -> `Pipe f
      | Direct f ->
        let writer : _ Direct_stream_writer.t =
          { id = Direct_stream_writer.Id.create ()
          ; state = Not_started (Queue.create ())
          ; closed = Ivar.create ()
          ; instance = T t
          ; query_id = id
          ; rpc
          ; groups = Bag.create ()
          ; stream_writer
          }
        in
        Hashtbl.set t.open_streaming_responses ~key:id ~data:(Direct writer);
        `Direct (f, writer)
    in
    let run_impl impl split_ok handle_ok on_err =
      let result =
        Rpc_result.try_with
          ?on_background_exception:on_exception.callback
          (fun () -> defer_result (data >>|~ impl))
          ~location:"server-side pipe_rpc computation"
      in
      Eager_deferred.upon result (function
        | Error (Rpc_error.Uncaught_exn sexp as err) ->
          Hashtbl.remove t.open_streaming_responses id;
          on_err ();
          write_streaming_initial t id bin_init_writer (Error err) `Final ~rpc ~error_mode;
          On_exception.handle_exn
            on_exception
            ~close_connection_monitor
            (Exn.create_s sexp)
        | Error err ->
          Hashtbl.remove t.open_streaming_responses id;
          on_err ();
          write_streaming_initial t id bin_init_writer (Error err) `Final ~rpc ~error_mode
        | Ok (Or_not_authorized.Not_authorized error) ->
          Hashtbl.remove t.open_streaming_responses id;
          on_err ();
          write_streaming_initial
            t
            id
            bin_init_writer
            (authorization_failure_result error ~rpc_kind:"pipe_rpc")
            `Final
            ~rpc
            ~error_mode
        | Ok (Authorized (Error error)) ->
          Hashtbl.remove t.open_streaming_responses id;
          on_err ();
          write_streaming_initial t id bin_init_writer (Ok error) `Final ~rpc ~error_mode
        | Ok (Authorized (Ok ok)) ->
          let initial, rest = split_ok ok in
          write_streaming_initial
            t
            id
            bin_init_writer
            (Ok initial)
            `Will_stream
            ~rpc
            ~error_mode;
          handle_ok rest)
    in
    match impl_with_state with
    | `Pipe f ->
      run_impl
        (fun data -> f t.connection_state data)
        Fn.id
        (fun pipe_r ->
          Hashtbl.set t.open_streaming_responses ~key:id ~data:(Pipe pipe_r);
          don't_wait_for
            (Protocol_writer.Unsafe_for_cached_bin_writer.transfer
               t.writer
               pipe_r
               (fun data ->
               let bin_writer_message = Cached_bin_writer.prep_write stream_writer data in
               unsafe_write_message_for_cached_bin_writer
                 t
                 ~id
                 ~rpc
                 ~kind:Streaming_update
                 bin_writer_message
                 (stream_writer, data)));
          Pipe.closed pipe_r
          >>> fun () ->
          Pipe.upstream_flushed pipe_r
          >>> function
          | `Ok | `Reader_closed ->
            write_streaming_eof
              t
              id
              P.Stream_response_data.bin_writer_nat0_t
              (Ok `Eof)
              ~rpc;
            Hashtbl.remove t.open_streaming_responses id)
        Fn.id
    | `Direct (f, writer) ->
      run_impl
        (fun data -> f t.connection_state data writer)
        (fun x -> x, ())
        (fun () -> Direct_stream_writer.start writer)
        (fun () -> Direct_stream_writer.close writer)
  ;;

  let apply_implementation
    t
    implementation
    ~(query : Nat0.t P.Query.t)
    ~read_buffer
    ~read_buffer_pos_ref
    ~close_connection_monitor
    ~on_exception
    ~message_bytes_for_tracing
    : _ Transport.Handler_result.t
    =
    let id = query.id in
    let rpc =
      ({ name = P.Rpc_tag.to_string query.tag; version = query.version } : Description.t)
    in
    let emit_regular_query_tracing_event () =
      write_event
        t
        { event = Received Query
        ; rpc = Some { name = P.Rpc_tag.to_string query.tag; version = query.version }
        ; id = (query.id :> Int63.t)
        ; payload_bytes = message_bytes_for_tracing
        }
    in
    match implementation with
    | Implementation.F.One_way (bin_query_reader, f) ->
      emit_regular_query_tracing_event ();
      let query_contents =
        bin_read_from_bigstring
          bin_query_reader
          read_buffer
          ~pos_ref:read_buffer_pos_ref
          ~len:query.data
          ~location:"server-side one-way rpc message un-bin-io'ing"
      in
      (match query_contents with
       | Error _ as err -> Stop err
       | Ok q ->
         let result : _ Transport_intf.Handler_result.t =
           try
             (* We don't close connections on unauthorized RPCs. Since there is no way to
                communicate failure back to the sender in one-way rpcs, we do nothing. The
                deferred (and any deferred exceptions) are dropped here as a consequence of
                [lift_deferred] existing for one-way rpcs. We use [Eager_deferred] so in
                the non-blocking cases this will still run synchronously. *)
             let (_ : _ Or_not_authorized.t Deferred.t) = f t.connection_state q in
             Continue
           with
           | exn ->
             On_exception.handle_exn on_exception ~close_connection_monitor exn;
             if on_exception.close_connection_if_no_return_value
             then
               Stop
                 (Rpc_result.uncaught_exn
                    exn
                    ~location:"server-side one-way rpc computation")
             else Continue
         in
         write_event
           t
           { event = Sent (Response One_way_so_no_response)
           ; rpc = Some rpc
           ; id :> Int63.t
           ; payload_bytes = 0
           };
         result)
    | Implementation.F.One_way_expert f ->
      emit_regular_query_tracing_event ();
      let result : _ Transport_intf.Handler_result.t =
        try
          let len = (query.data :> int) in
          let (_ : _ Or_not_authorized.t Deferred.t) =
            f t.connection_state read_buffer ~pos:!read_buffer_pos_ref ~len
          in
          read_buffer_pos_ref := !read_buffer_pos_ref + len;
          Continue
        with
        | exn ->
          On_exception.handle_exn on_exception ~close_connection_monitor exn;
          if on_exception.close_connection_if_no_return_value
          then
            Stop
              (Rpc_result.uncaught_exn
                 exn
                 ~location:"server-side one-way rpc expert computation")
          else Continue
      in
      write_event
        t
        { event = Sent (Response One_way_so_no_response)
        ; rpc = Some { name = P.Rpc_tag.to_string query.tag; version = query.version }
        ; id :> Int63.t
        ; payload_bytes = 0
        };
      result
    | Implementation.F.Rpc
        (bin_query_reader, bin_response_writer, f, error_mode, result_mode) ->
      emit_regular_query_tracing_event ();
      let query_contents =
        bin_read_from_bigstring
          bin_query_reader
          read_buffer
          ~pos_ref:read_buffer_pos_ref
          ~len:query.data
          ~location:"server-side rpc query un-bin-io'ing"
      in
      let or_not_authorized_to_rpc_result = function
        | Or_not_authorized.Authorized result -> Ok result
        | Not_authorized error -> authorization_failure_result error ~rpc_kind:"rpc"
      in
      (match result_mode with
       | Implementation.F.Blocking ->
         (match
            Result.bind query_contents ~f:(fun query ->
              f t.connection_state query |> or_not_authorized_to_rpc_result)
          with
          | response ->
            write_single_response t id bin_response_writer response ~rpc ~error_mode
          | exception exn ->
            (* In the [Deferred] branch we use [Monitor.try_with], which includes
               backtraces when it catches an exception. For consistency, we also get
               backtraces here. *)
            let backtrace = Backtrace.Exn.most_recent () in
            let sexp =
              [%sexp
                { location = "server-side blocking rpc computation"
                ; exn : exn
                ; backtrace : Backtrace.t
                }]
            in
            write_single_response
              t
              id
              bin_response_writer
              (Error (Rpc_error.Uncaught_exn sexp))
              ~rpc
              ~error_mode;
            On_exception.handle_exn on_exception ~close_connection_monitor exn)
       | Implementation.F.Deferred ->
         let result =
           (* We generally try to write a response before handling [on_exception] so if we
              are closing the connection we still actually send the response back. When we
              pass [on_exception.callback] here, we are making it possible for raised
              exceptions not to be written back to the client (e.g. if the implementation
              raises both asynchronously and synchronously). This would be hard to handle
              in a more principled way. *)
           Rpc_result.try_with
             ?on_background_exception:on_exception.callback
             ~run:`Now
             ~location:"server-side rpc computation"
             (fun () ->
             match query_contents with
             | Error err -> Deferred.return (Error err)
             | Ok query ->
               Eager_deferred.map (f t.connection_state query) ~f:(fun result ->
                 Ok result))
         in
         let handle_result result =
           let write_response response =
             let rpc =
               ({ name = P.Rpc_tag.to_string query.tag; version = query.version }
                 : Description.t)
             in
             write_single_response
               t
               id
               bin_response_writer
               response
               ~rpc
               ~error_mode [@nontail]
           in
           match result with
           | Error error as result ->
             write_response result;
             maybe_dispatch_on_exception error on_exception ~close_connection_monitor
           | Ok (Or_not_authorized.Authorized result) -> write_response (Ok result)
           | Ok (Not_authorized error) ->
             write_response (authorization_failure_result error ~rpc_kind:"rpc")
         in
         (* In the common case that the implementation returns a value immediately, we will
            write the response immediately as well (this is also why the above [try_with]
            has [~run:`Now]).  This can be a big performance win for servers that get many
            queries in a single Async cycle. *)
         (match Deferred.peek result with
          | None -> result >>> handle_result
          | Some result -> handle_result result));
      Continue
    | Implementation.F.Legacy_menu_rpc menu ->
      emit_regular_query_tracing_event ();
      let query_contents =
        bin_read_from_bigstring
          Menu.Stable.V1.bin_reader_query
          read_buffer
          ~pos_ref:read_buffer_pos_ref
          ~len:query.data
          ~location:"server-side rpc query un-bin-io'ing"
      in
      let error_mode = Implementation.F.Always_ok in
      (try
         Result.map query_contents ~f:(fun () ->
           (* We have to map down to the V1 menu since that's the type that old clients
              are expecting (but we need to be able to self-dispatch the menu to get the
              V2 response for connection metadata). *)
           menu |> force |> List.map ~f:fst |> Menu.Stable.V1.response_of_model)
         |> write_single_response t id Menu.Stable.V1.bin_writer_response ~rpc ~error_mode
       with
       | exn ->
         (* In the [Deferred] branch for [Rpc], we use [Monitor.try_with], which includes
            backtraces when it catches an exception. For consistency, we also get
            backtraces here. *)
         let backtrace = Backtrace.Exn.most_recent () in
         let sexp =
           [%sexp
             { location = "server-side blocking rpc computation"
             ; exn : exn
             ; backtrace : Backtrace.t
             }]
         in
         write_single_response
           t
           id
           Menu.Stable.V1.bin_writer_response
           (Error (Rpc_error.Uncaught_exn sexp))
           ~rpc
           ~error_mode;
         On_exception.handle_exn on_exception ~close_connection_monitor exn);
      Continue
    | Implementation.F.Rpc_expert (f, result_mode) ->
      emit_regular_query_tracing_event ();
      let responder = Implementation.Expert.Responder.create query.id t.writer in
      let d =
        (* We need the [Monitor.try_with] even for the blocking mode as the implementation
           might return [Delayed_reponse], so we don't bother optimizing the blocking
           mode. *)
        let rest =
          match on_exception.callback with
          | None -> `Log
          | Some callback -> `Call callback
        in
        Monitor.try_with ~rest ~run:`Now (fun () ->
          let len = (query.data :> int) in
          let result =
            f t.connection_state responder read_buffer ~pos:!read_buffer_pos_ref ~len
          in
          match result_mode with
          | Implementation.F.Deferred -> result
          | Implementation.F.Blocking -> Deferred.return result)
      in
      let computation_failure_result exn =
        Rpc_result.uncaught_exn exn ~location:"server-side rpc expert computation"
      in
      let handle_exn result =
        let result =
          if responder.responded
          then result
          else (
            write_single_response
              t
              id
              bin_writer_unit
              result
              ~rpc:{ name = P.Rpc_tag.to_string query.tag; version = query.version }
              ~error_mode:Always_ok;
            Ok ())
        in
        result
      in
      let check_responded () =
        if responder.responded
        then (
          write_event
            t
            { event = Sent (Response Expert_single_succeeded_or_failed)
            ; rpc = Some { name = P.Rpc_tag.to_string query.tag; version = query.version }
            ; id :> Int63.t
            ; payload_bytes = 0
            };
          Ok ())
        else
          handle_exn
            (computation_failure_result (Failure "Expert implementation did not reply"))
      in
      let d =
        let open Eager_deferred.Let_syntax in
        match%map d with
        | Ok (Authorized result) ->
          let d =
            match result with
            | Replied -> Deferred.unit
            | Delayed_response d -> d
          in
          if Deferred.is_determined d
          then check_responded ()
          else (
            upon d (fun () ->
              check_responded ()
              |> Rpc_result.or_error
                   ~rpc_description:
                     { name = P.Rpc_tag.to_string query.tag; version = query.version }
                   ~connection_description:t.connection_description
                   ~connection_close_started:t.connection_close_started
              |> ok_exn);
            Ok ())
        | Ok (Not_authorized error) ->
          handle_exn (authorization_failure_result error ~rpc_kind:"rpc expert")
        | Error exn ->
          let result = handle_exn (computation_failure_result exn) in
          On_exception.handle_exn on_exception ~close_connection_monitor exn;
          result
      in
      (match Deferred.peek d with
       | None ->
         Wait
           (let%map r = d in
            ok_exn
              (Rpc_result.or_error
                 ~rpc_description:
                   { name = P.Rpc_tag.to_string query.tag; version = query.version }
                 ~connection_description:t.connection_description
                 ~connection_close_started:t.connection_close_started
                 r))
       | Some result ->
         (match result with
          | Ok () -> Continue
          | Error _ -> Stop result))
    | Implementation.F.Streaming_rpc streaming_rpc ->
      let stream_query =
        bin_read_from_bigstring
          P.Stream_query.bin_reader_nat0_t
          read_buffer
          ~pos_ref:read_buffer_pos_ref
          ~len:query.data
          ~location:"server-side pipe_rpc stream_query un-bin-io'ing"
          ~add_len:(function
          | `Abort -> 0
          | `Query (len : Nat0.t) -> (len :> int))
      in
      (match stream_query with
       | Error _ as error ->
         (* There are roughly four things that can happen here:
            1. The client thinks they are sending a regular rpc so we fail to parse
            2. The client sent a streaming rpc but the message was corrupted
            3. The client sent an abort but the message was corrupted
            4. The client thinks they are sending a one-way rpc so we fail to parse

            In cases 1 and 2, the client will expect a response and in cases 3 and 4 they
            won’t. We do send a response here with an error. In case 1 and 2 this is
            returned to the caller. In case 3 and 4 the client closes its connection
            complaining about the error or an unknown query id. *)
         emit_regular_query_tracing_event ();
         write_streaming_initial
           t
           id
           [%bin_writer: Nothing.t]
           error
           `Final
           ~rpc:{ name = P.Rpc_tag.to_string query.tag; version = query.version }
           ~error_mode:Always_ok
       | Ok `Abort ->
         write_event
           t
           { event = Received Abort_streaming_rpc_query
           ; rpc = Some { name = P.Rpc_tag.to_string query.tag; version = query.version }
           ; id = (query.id :> Int63.t)
           ; payload_bytes = message_bytes_for_tracing
           };
         (* Note that there's some delay between when we receive a pipe RPC query and
            when we put something in [open_streaming_responses] (we wait for
            a user-supplied function to return). During this time, an abort message would
            just be ignored. The dispatcher can't abort the query while this is
            happening, though, since the interface doesn't expose the ID required to
            abort the query until after a response has been returned. *)
         Option.iter (Hashtbl.find t.open_streaming_responses query.id) ~f:(function
           | Pipe pipe -> Pipe.close_read pipe
           | Direct w -> Direct_stream_writer.close w)
       | Ok (`Query len) ->
         emit_regular_query_tracing_event ();
         apply_streaming_implementation
           t
           streaming_rpc
           ~len
           ~read_buffer
           ~read_buffer_pos_ref
           ~id
           ~rpc:{ name = P.Rpc_tag.to_string query.tag; version = query.version }
           ~on_exception
           ~close_connection_monitor);
      Continue
  ;;

  let flush (T t) =
    assert (not t.stopped);
    let producers_flushed =
      Hashtbl.fold t.open_streaming_responses ~init:[] ~f:(fun ~key:_ ~data acc ->
        match data with
        | Direct _ -> acc
        | Pipe pipe -> Deferred.ignore_m (Pipe.upstream_flushed pipe) :: acc)
    in
    Deferred.all_unit producers_flushed
  ;;

  let stop (T t) =
    t.stopped <- true;
    Hashtbl.iter t.open_streaming_responses ~f:(function
      | Direct writer ->
        (* Don't remove the writer from the instance, as that would modify the hashtable
           that we are currently iterating over. *)
        Direct_stream_writer.close_without_removing_from_instance writer
      | Pipe _ -> ());
    Hashtbl.clear t.open_streaming_responses
  ;;

  let handle_unknown_rpc on_unknown_rpc error t query : _ Transport.Handler_result.t =
    match on_unknown_rpc with
    | `Continue -> Continue
    | `Raise -> Rpc_error.raise error t.connection_description
    | `Close_connection -> Stop (Ok ())
    | `Call f ->
      (match
         f
           t.connection_state
           ~rpc_tag:(P.Rpc_tag.to_string query.P.Query.tag)
           ~version:query.version
       with
       | `Close_connection -> Stop (Ok ())
       | `Continue -> Continue)
  ;;

  let handle_query_internal
    t
    ~(query : Nat0.t P.Query.t)
    ~read_buffer
    ~read_buffer_pos_ref
    ~close_connection_monitor
    ~message_bytes_for_tracing
    =
    let { implementations; on_unknown_rpc } = t.implementations in
    let description : Description.t =
      { name = P.Rpc_tag.to_string query.tag; version = query.version }
    in
    match t.last_dispatched_implementation with
    | Some (last_desc, implementation) when Description.equal last_desc description ->
      apply_implementation
        t
        implementation.f
        ~query
        ~read_buffer
        ~read_buffer_pos_ref
        ~close_connection_monitor
        ~on_exception:implementation.on_exception
        ~message_bytes_for_tracing
    | None | Some _ ->
      (match Hashtbl.find implementations description with
       | Some implementation ->
         t.last_dispatched_implementation <- Some (description, implementation);
         apply_implementation
           t
           implementation.f
           ~on_exception:implementation.on_exception
           ~query
           ~read_buffer
           ~read_buffer_pos_ref
           ~close_connection_monitor
           ~message_bytes_for_tracing
       | None ->
         write_event
           t
           { event = Received Query
           ; rpc = Some { name = P.Rpc_tag.to_string query.tag; version = query.version }
           ; id = (query.id :> Int63.t)
           ; payload_bytes = message_bytes_for_tracing
           };
         (match on_unknown_rpc with
          | `Expert impl ->
            let { P.Query.tag; version; id; metadata; data = len } = query in
            let rpc_tag = P.Rpc_tag.to_string tag in
            let d =
              let responder = Responder.create id t.writer in
              impl
                t.connection_state
                ~rpc_tag
                ~version
                ~metadata
                responder
                read_buffer
                ~pos:!read_buffer_pos_ref
                ~len:(len :> int)
            in
            (* This event is a bit of a lie, e.g. we still write this event if the impl
               did not respond to the event, which I guess might happen if this handler
               were used to proxy one-way rpcs. Nevertheless, it does mean there will be
               response events for each incoming query (so long as [impl] doesn’t raise).
            *)
            write_event
              t
              { event = Sent (Response Expert_single_succeeded_or_failed)
              ; rpc = Some { name = rpc_tag; version }
              ; id :> Int63.t
              ; payload_bytes = 0
              };
            if Deferred.is_determined d then Continue else Wait d
          | (`Continue | `Raise | `Close_connection | `Call _) as on_unknown_rpc ->
            let error = Rpc_error.Unimplemented_rpc (query.tag, `Version query.version) in
            write_single_response
              t
              query.id
              P.Message.bin_writer_nat0_t
              (Error error)
              ~rpc:{ name = P.Rpc_tag.to_string query.tag; version = query.version }
              ~error_mode:Always_ok;
            handle_unknown_rpc on_unknown_rpc error t query))
  ;;

  let handle_query
    (T t)
    ~(query : Nat0.t P.Query.t)
    ~read_buffer
    ~read_buffer_pos_ref
    ~close_connection_monitor
    ~message_bytes_for_tracing
    =
    if t.stopped || Protocol_writer.is_closed t.writer
    then Transport.Handler_result.Stop (Ok ())
    else (
      let old_ctx = Async_kernel_scheduler.current_execution_context () in
      let new_ctx =
        t.on_receive
          { name = P.Rpc_tag.to_string query.tag; version = query.version }
          ~query_id:query.id
          query.metadata
          old_ctx
      in
      (* The reason for the weirdness here is that there is no equivalent of
         [Async_kernel_scheduler.within_context] that doesn’t return a [Result.t]. The
         difference between the code below and calling [within_context] is that errors are
         sent to the monitor for [old_ctx] rather than the monitor for [new_ctx], and we
         reraise here instead of working out what to do if [handle_query_internal] raises.
      *)
      Async_kernel_scheduler.Expert.set_execution_context new_ctx;
      protect
        ~f:(fun () ->
          handle_query_internal
            t
            ~query
            ~read_buffer
            ~read_buffer_pos_ref
            ~close_connection_monitor
            ~message_bytes_for_tracing)
        ~finally:(fun () -> Async_kernel_scheduler.Expert.set_execution_context old_ctx))
  ;;
end

module Direct_stream_writer = Instance.Direct_stream_writer

let create ~implementations:i's ~on_unknown_rpc =
  (* Make sure the tags are unique. *)
  let implementations = Description.Table.create ~size:10 () in
  let dups = Description.Hash_set.create ~size:10 () in
  List.iter i's ~f:(fun (i : _ Implementation.t) ->
    let description =
      { Description.name = P.Rpc_tag.to_string i.tag; version = i.version }
    in
    match Hashtbl.add implementations ~key:description ~data:i with
    | `Ok -> ()
    | `Duplicate -> Hash_set.add dups description);
  if not (Hash_set.is_empty dups)
  then Error (`Duplicate_implementations (Hash_set.to_list dups))
  else Ok { implementations; on_unknown_rpc :> _ on_unknown_rpc_with_expert }
;;

let instantiate
  t
  ~connection_description
  ~connection_close_started
  ~connection_state
  ~writer
  ~events
  : Instance.t
  =
  T
    { implementations = t
    ; writer
    ; events
    ; open_streaming_responses = Hashtbl.Poly.create ~size:10 ()
    ; connection_state
    ; connection_description
    ; connection_close_started
    ; stopped = false
    ; last_dispatched_implementation = None
    ; on_receive =
        (fun (_ : Description.t) ~query_id:(_ : P.Query_id.t) metadata ctx ->
          Rpc_metadata.Private.set metadata ctx)
    }
;;

exception Duplicate_implementations of Description.t list [@@deriving sexp]

let create_exn ~implementations ~on_unknown_rpc =
  match create ~implementations ~on_unknown_rpc with
  | Ok x -> x
  | Error (`Duplicate_implementations dups) -> raise (Duplicate_implementations dups)
;;

let null () = create_exn ~implementations:[] ~on_unknown_rpc:`Raise

let add_exn t (implementation : _ Implementation.t) =
  let desc : Description.t =
    { name = P.Rpc_tag.to_string implementation.tag; version = implementation.version }
  in
  let implementations = Hashtbl.copy t.implementations in
  match Hashtbl.add implementations ~key:desc ~data:implementation with
  | `Duplicate -> raise (Duplicate_implementations [ desc ])
  | `Ok -> { t with implementations }
;;

let add t implementation = Or_error.try_with (fun () -> add_exn t implementation)

let remove_exn t description =
  let implementations = Hashtbl.copy t.implementations in
  let implementation = Hashtbl.find_exn implementations description in
  Hashtbl.remove implementations description;
  implementation, { t with implementations }
;;

let find t description = Hashtbl.find t.implementations description

let lift { implementations; on_unknown_rpc } ~f =
  let implementations = Hashtbl.map implementations ~f:(Implementation.lift ~f) in
  let on_unknown_rpc =
    match on_unknown_rpc with
    | (`Raise | `Continue | `Close_connection) as x -> x
    | `Call call -> `Call (fun state -> call (f state))
    | `Expert expert -> `Expert (fun state -> expert (f state))
  in
  { implementations; on_unknown_rpc }
;;

module Expert = struct
  module Responder = Responder

  module Rpc_responder = struct
    type t = Responder.t

    let cannot_send r =
      failwiths
        ~here:[%here]
        "Message cannot be sent"
        r
        [%sexp_of: unit Transport.Send_result.t]
    ;;

    let mark_responded (t : t) =
      if t.responded
      then failwiths ~here:[%here] "Already responded" t [%sexp_of: Responder.t];
      t.responded <- true
    ;;

    let schedule (t : t) buf ~pos ~len =
      mark_responded t;
      match
        Protocol_writer.send_expert_response
          t.writer
          t.query_id
          ~buf
          ~pos
          ~len
          ~send_bin_prot_and_bigstring:
            Transport.Writer.send_bin_prot_and_bigstring_non_copying
      with
      | Sent { result = d; bytes = (_ : int) } -> `Flushed d
      | Closed -> `Connection_closed
      | Message_too_big _ as r ->
        cannot_send ([%globalize: unit Transport.Send_result.t] r)
    ;;

    let handle_send_result : unit Transport.Send_result.t -> unit = function
      | Sent { result = (); bytes = (_ : int) } | Closed -> ()
      | Message_too_big _ as r ->
        cannot_send ([%globalize: unit Transport.Send_result.t] r)
    ;;

    let write_bigstring (t : t) buf ~pos ~len =
      mark_responded t;
      Protocol_writer.send_expert_response
        t.writer
        t.query_id
        ~buf
        ~pos
        ~len
        ~send_bin_prot_and_bigstring:Transport.Writer.send_bin_prot_and_bigstring
      |> handle_send_result;
      ()
    ;;

    let write_error (t : t) error =
      mark_responded t;
      let data =
        Rpc_result.uncaught_exn
          ~location:"server-side raw rpc computation"
          (Error.to_exn error)
      in
      Protocol_writer.send_response
        t.writer
        { id = t.query_id; data }
        ~bin_writer_response:Nothing.bin_writer_t
      |> handle_send_result;
      ()
    ;;

    let write_bin_prot (t : t) bin_writer_a a =
      mark_responded t;
      Protocol_writer.send_response
        t.writer
        { id = t.query_id; data = Ok a }
        ~bin_writer_response:bin_writer_a
      |> handle_send_result;
      ()
    ;;
  end

  let create_exn = create_exn
end
