open Core
open Async_kernel
open Util
module P = Protocol

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
    -> metadata:Rpc_metadata.V2.t option
    -> Responder.t
    -> Bigstring.t
    -> pos:int
    -> len:int
    -> unit Deferred.t
  ]

type 'connection_state t =
  { implementations : 'connection_state Implementation.t Description.Table.t
  ; on_unknown_rpc : 'connection_state on_unknown_rpc_with_expert
  ; on_exception : On_exception.t
  }

type 'connection_state implementations = 'connection_state t

let descriptions t =
  Hashtbl.keys t.implementations |> List.sort ~compare:[%compare: Description.t]
;;

let descriptions_and_shapes ?exclude_name t =
  Hashtbl.fold t.implementations ~init:[] ~f:(fun ~key ~data acc ->
    match exclude_name with
    | Some name when String.equal name key.name -> acc
    | _ -> (key, Implementation.digests data) :: acc)
  |> (* We know that descriptions are unique within an [Implementations] *)
  List.sort ~compare:(Comparable.lift [%compare: Description.t] ~f:fst)
;;

module Instance = struct
  module Streaming_response = struct
    type t =
      | Pipe : _ Pipe.Reader.t Set_once.t -> t
      | Direct : (_ Implementation_types.Direct_stream_writer.t[@sexp.opaque]) -> t
    [@@deriving sexp_of]
  end

  type 'a unpacked =
    { implementations : ('a implementations[@sexp.opaque])
    ; menu : Menu.t option
    ; writer : Protocol_writer.t
    ; tracing_events : (local_ Tracing_event.t -> unit) Bus.Read_write.t
    ; no_open_queries_event : (unit, read_write) Bvar.t
    ; open_streaming_responses : (Protocol.Query_id.t, Streaming_response.t) Hashtbl.t
    ; mutable open_queries : int
    ; mutable stopped : bool
    ; connection_state : 'a
    ; connection_description : Info.t
    ; connection_close_started : Close_reason.t Deferred.t
    ; connection_close_started_info : Info.t Deferred.t
    ; mutable last_dispatched_implementation :
        (Description.t * ('a Implementation.t[@sexp.opaque]) * Protocol.Impl_menu_index.t)
          option
    ; on_receive :
        local_ Description.t
        -> query_id:P.Query_id.t
        -> Rpc_metadata.V2.t option
        -> Execution_context.t
        -> Execution_context.t
    }
  [@@deriving sexp_of]

  type t = T : _ unpacked -> t [@@unboxed]

  let sexp_of_t (T t) = [%sexp_of: _ unpacked] t

  let write_tracing_event t (local_ event) =
    if not (Bus.is_closed t.tracing_events) then Bus.write_local t.tracing_events event
  ;;

  let write_response'
    (type a)
    t
    id
    impl_menu_index
    bin_writer_data
    (data : a Rpc_result.t)
    ~rpc
    ~(error_mode : a Implementation_mode.Error_mode.t)
    ~ok_kind
    =
    if not t.stopped
    then (
      let kind : Tracing_event.Sent_response_kind.t =
        (* We skip trying to determine if the response is an error if there are no
           subscribers as calling arbitrary functions may be expensive. *)
        if Bus.num_subscribers t.tracing_events = 0
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
      (Protocol_writer.Response.send
         t.writer
         id
         impl_menu_index
         ~data
         ~bin_writer_response:bin_writer_data
       |> Protocol_writer.Response.handle_send_result t.writer id impl_menu_index rpc kind
      )
      [@nontail])
  ;;

  let cleanup_and_write_streaming_eof
    t
    id
    impl_menu_index
    (data : [< `Eof ] Rpc_result.t)
    ~rpc
    =
    if Hashtbl.mem t.open_streaming_responses id
    then (
      write_response'
        t
        id
        impl_menu_index
        P.Stream_response_data.bin_writer_nat0_t
        (data :> P.Stream_response_data.nat0_t Rpc_result.t)
        ~rpc
        ~error_mode:Always_ok
        ~ok_kind:Streaming_closed;
      Hashtbl.remove t.open_streaming_responses id)
  ;;

  let write_single_response t id impl_menu_index bin_writer_data data ~rpc ~error_mode =
    write_response'
      t
      id
      impl_menu_index
      bin_writer_data
      data
      ~rpc
      ~error_mode
      ~ok_kind:Single_succeeded
  ;;

  let get_description_from_menu_rank (T t) rank = exclave_
    match t.menu with
    | None -> None
    | Some menu -> Menu.get menu rank
  ;;

  module Direct_stream_writer = struct
    module T = Implementation_types.Direct_stream_writer
    module State = T.State
    module Id = T.Id

    type 'a t = 'a T.t =
      { id : Id.t
      ; mutable state : 'a State.t
      ; started : unit Ivar.t
      ; closed : unit Ivar.t
      ; query_id : P.Query_id.t
      ; rpc : Description.t
      ; stream_writer : 'a Cached_streaming_response_writer.t
      ; groups : 'a group_entry Bag.t
      ; mutable instance_stopped : bool
      ; cleanup_and_write_streaming_eof : [ `Eof ] Rpc_result.t -> unit
      }

    and 'a group_entry = 'a T.group_entry =
      { group : 'a T.Group.t
      ; element_in_group : 'a t Bag.Elt.t
      }

    let started t = Ivar.read t.started
    let is_closed t = Ivar.is_full t.closed
    let closed t = Ivar.read t.closed
    let bin_writer t = Cached_streaming_response_writer.bin_writer t.stream_writer
    let flushed t = Cached_streaming_response_writer.flushed t.stream_writer

    (* [write_message_string] and [write_message] both allocate 3 words for the tuples.
       [write_message_expert] does not allocate. *)

    let write_message_string t x =
      if not t.instance_stopped
      then Cached_streaming_response_writer.write_string t.stream_writer x
    ;;

    let write_message t x =
      if not t.instance_stopped
      then Cached_streaming_response_writer.write t.stream_writer x
    ;;

    let write_message_expert t ~buf ~pos ~len =
      if not t.instance_stopped
      then Cached_streaming_response_writer.write_expert t.stream_writer ~buf ~pos ~len
    ;;

    let schedule_write_message_expert ~(here : [%call_pos]) t ~buf ~pos ~len = exclave_
      if not t.instance_stopped
      then
        Cached_streaming_response_writer.schedule_write_expert
          ~here
          t.stream_writer
          ~buf
          ~pos
          ~len
      else `Closed
    ;;

    let close ?(result = Ok `Eof) t =
      if not (is_closed t)
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
        | Not_started _ -> ()
        | Started ->
          (* If we've started then we'll clean up the [open_streaming_responses]. If we
             haven't started and [close] is called earlier than [start], [t.closed] will
             be full which will induce a [streaming_eof] in [start]. *)
          t.cleanup_and_write_streaming_eof result)
    ;;

    (* Handles initialization errors where [start] will never be called. It closes the
       writer and marks any pending [Expert.schedule_write] buffers as done so callers
       don't end up waiting forever for the buffers to be returned. *)
    let close_and_abandon_queued_writes t =
      close t;
      match t.state with
      | Not_started q ->
        Queue.iter q ~f:(function
          | Normal _ | Expert_string (_ : string) -> ()
          | Expert_schedule_bigstring
              { buf = (_ : Bigstring.t); pos = (_ : int); len = (_ : int); done_ } ->
            Ivar.fill_if_empty done_ ())
      | Started -> ()
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

    let write t x =
      match write_without_pushback t x with
      | `Closed -> `Closed
      | `Ok -> `Flushed (flushed t)
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

      let write t ~buf ~pos ~len =
        match write_without_pushback t ~buf ~pos ~len with
        | `Closed -> `Closed
        | `Ok -> `Flushed (flushed t)
      ;;

      let schedule_write t ~buf ~pos ~len = exclave_
        if Ivar.is_full t.closed
        then `Closed
        else (
          match t.state with
          | Started -> schedule_write_message_expert t ~buf ~pos ~len
          | Not_started q ->
            let done_ = Ivar.create () in
            Queue.enqueue q (Expert_schedule_bigstring { buf; pos; len; done_ });
            `Flushed { global = Ivar.read done_ })
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
             | `Flushed { global = d } ->
               (* Fill [done_] when either the flush completes or the DSW is closed. This
                  ensures the buffer is marked safe to reuse even if the connection is
                  closed before the transport flushes. *)
               Eager_deferred.upon
                 (Deferred.any_unit [ d; closed t ])
                 (Ivar.fill_exn done_)
             | `Closed -> Ivar.fill_exn done_ ()));
        Ivar.fill_exn t.started ();
        if Ivar.is_full t.closed then t.cleanup_and_write_streaming_eof (Ok `Eof)
    ;;
  end

  let maybe_dispatch_on_exception
    (error : Rpc_error.t)
    on_exception
    description
    ~close_connection_monitor
    =
    match error with
    | Uncaught_exn sexp ->
      let (`Stop | `Continue) =
        On_exception.handle_exn_before_implementation_returns
          on_exception
          (Exn.create_s sexp)
          description
          ~close_connection_monitor
      in
      ()
    | (_ : Rpc_error.t) -> ()
  ;;

  let authorization_failure_result error ~rpc_kind =
    let exn = Error.to_exn error in
    Rpc_result.authorization_failure
      exn
      ~location:[%string "server-side %{rpc_kind} authorization"]
  ;;

  let lift_failure_result error ~rpc_kind =
    let exn = Error.to_exn error in
    Rpc_result.lift_error exn ~location:[%string "server-side %{rpc_kind} lift"]
  ;;

  let apply_streaming_implementation
    t
    { Implementation.F.bin_query_reader
    ; bin_init_writer
    ; bin_update_writer
    ; impl
    ; error_mode
    ; leave_open_on_exception
    ; here
    }
    ~len
    ~read_buffer
    ~read_buffer_pos_ref
    ~id
    ~impl_menu_index
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
    let stream_writer =
      Cached_streaming_response_writer.create
        t.writer
        id
        impl_menu_index
        rpc
        ~bin_writer:bin_update_writer
    in
    let map_streaming_impl ~f =
      Eager_deferred.map ~f:(Or_error.map ~f:(Or_not_authorized.map ~f:(Result.map ~f)))
    in
    let write_streaming_initial result is_final =
      if Hashtbl.mem t.open_streaming_responses id
      then (
        let ok_kind : Tracing_event.Sent_response_kind.t =
          match is_final with
          | `Final -> Single_or_streaming_rpc_error_or_exn
          | `Will_stream -> Streaming_initial
        in
        write_response'
          t
          id
          impl_menu_index
          bin_init_writer
          result
          ~rpc
          ~error_mode
          ~ok_kind)
    in
    let partial_impl, after_initial_response =
      match impl with
      | Pipe f ->
        let pipe_placeholder = Set_once.create () in
        Hashtbl.set t.open_streaming_responses ~key:id ~data:(Pipe pipe_placeholder);
        let impl data =
          f t.connection_state data
          |> map_streaming_impl ~f:(fun (initial_message, pipe_reader) ->
            (match Hashtbl.find t.open_streaming_responses id with
             | Some (Pipe _) ->
               (* Can't set the [pipe_placeholder] through [t.open_streaming_responses]
                  since the type parameters will escape the scope of this closure. *)
               Set_once.set_exn pipe_placeholder pipe_reader
             | None | Some (Direct _) -> Pipe.close_read pipe_reader);
            initial_message)
        in
        let after_initial_response () =
          Set_once.get pipe_placeholder
          |> Option.iter ~f:(fun pipe_reader ->
            let transfer_to_pipe =
              Protocol_writer.Unsafe_for_cached_streaming_response_writer.transfer
                t.writer
                pipe_reader
                (fun data ->
                   if not t.stopped
                   then Cached_streaming_response_writer.write stream_writer data)
            in
            don't_wait_for transfer_to_pipe;
            Pipe.closed pipe_reader
            >>> fun () ->
            Pipe.upstream_flushed pipe_reader
            >>> function
            | `Ok | `Reader_closed ->
              cleanup_and_write_streaming_eof t id impl_menu_index (Ok `Eof) ~rpc)
        in
        impl, after_initial_response
      | Direct f ->
        let writer : _ Direct_stream_writer.t =
          { id = Direct_stream_writer.Id.create ()
          ; state = Not_started (Queue.create ())
          ; started = Ivar.create ()
          ; closed = Ivar.create ()
          ; query_id = id
          ; rpc
          ; groups = Bag.create ()
          ; stream_writer
          ; instance_stopped = t.stopped
          ; cleanup_and_write_streaming_eof =
              (fun result ->
                cleanup_and_write_streaming_eof t id impl_menu_index result ~rpc)
          }
        in
        Hashtbl.set t.open_streaming_responses ~key:id ~data:(Direct writer);
        let impl data = f t.connection_state data writer in
        let after_initial_response () = Direct_stream_writer.start writer in
        impl, after_initial_response
    in
    let is_final_of_result
      =
      (* We distinguish between [Uncaught_exn] and other errors since we need to run
         [On_exception] handling. *)
      function
      | Error (Rpc_error.Uncaught_exn sexp) -> `Final_with_uncaught_exn sexp
      | Error error -> `Final (Error error)
      | Ok (Ok (Or_not_authorized.Not_authorized error)) ->
        `Final (authorization_failure_result error ~rpc_kind:"pipe_rpc")
      | Ok (Error error) -> `Final (lift_failure_result error ~rpc_kind:"pipe_rpc")
      | Ok (Ok (Authorized (Error error))) -> `Final (Ok error)
      | Ok (Ok (Authorized (Ok ok))) -> `Will_stream ok
    in
    let on_initialization_error result =
      (* [Direct_stream_writer] currently uses the existence of the writer in
         [t.open_streaming_responses] when deciding whether to send an [Eof]. On an
         initialization error we don't send [Eof] so we [Hashtbl.find_and_remove]. *)
      write_streaming_initial result `Final;
      match Hashtbl.find_and_remove t.open_streaming_responses id with
      | Some (Direct writer) ->
        Direct_stream_writer.close_and_abandon_queued_writes writer
      | Some (Pipe pipe_placeholder) ->
        Set_once.get pipe_placeholder |> Option.iter ~f:Pipe.close_read
      | None -> ()
    in
    let close_on_background_exn rpc_error =
      (* [cleanup_and_write_streaming_eof] takes care of removing [id] from
         [t.open_streaming_responses]. [Pipe] calls it directly here and
         [Direct_stream_writer.close] eventually calls it. *)
      match Hashtbl.find t.open_streaming_responses id with
      | None ->
        (* It's possible that we've synchronously errored (and sent an EOF) so we don't
           need to do anything in this case. *)
        ()
      | Some (Pipe pipe_placeholder) ->
        (match Set_once.get pipe_placeholder with
         | None -> cleanup_and_write_streaming_eof t id impl_menu_index rpc_error ~rpc
         | Some pipe_reader ->
           let clean_up_and_close_read_on_pipe =
             match%map Pipe.downstream_flushed pipe_reader with
             | `Reader_closed ->
               (* If we have a pipe in the [pipe_placeholder], then the pipe's [impl] was
                  successful and no background exception was raised before the [impl]
                  succeeded. So [after_initial_response] will notice that the pipe is
                  closed and write [Ok `Eof]. *)
               ()
             | `Ok ->
               cleanup_and_write_streaming_eof t id impl_menu_index rpc_error ~rpc;
               Pipe.close_read pipe_reader
           in
           don't_wait_for clean_up_and_close_read_on_pipe)
      | Some (Direct direct_stream_writer) ->
        Direct_stream_writer.close direct_stream_writer ~result:rpc_error
    in
    let result =
      Rpc_result.try_with
        ~here
        (fun () -> defer_result (Result.map data ~f:partial_impl))
        ~location:"server-side pipe_rpc computation"
        ~on_background_exception:
          (Some
             (On_exception.Background_monitor_rest.Expert.merge
                (`Call
                  (fun exn ->
                    if not leave_open_on_exception
                    then
                      close_on_background_exn
                        (Error (Rpc_error.Uncaught_exn (Exn.sexp_of_t exn)))))
                (On_exception.to_background_monitor_rest
                   on_exception
                   rpc
                   ~close_connection_monitor)))
      |> Eager_deferred.map ~f:is_final_of_result
    in
    Eager_deferred.upon result (function
      | `Final_with_uncaught_exn sexp ->
        on_initialization_error (Error (Rpc_error.Uncaught_exn sexp));
        let (`Stop | `Continue) =
          On_exception.handle_exn_before_implementation_returns
            on_exception
            (Exn.create_s sexp)
            rpc
            ~close_connection_monitor
        in
        ()
      | `Final result -> on_initialization_error result
      | `Will_stream initial_message ->
        write_streaming_initial (Ok initial_message) `Will_stream;
        after_initial_response ())
  ;;

  let[@inline] increment_open_queries
    (type a b)
    t
    (result_mode : (a, b) Implementation_mode.Result_mode.t)
    =
    match result_mode with
    | Deferred -> t.open_queries <- t.open_queries + 1
    | Blocking -> ()
  ;;

  let[@inline] decrement_open_queries
    (type a b)
    t
    (result_mode : (a, b) Implementation_mode.Result_mode.t)
    =
    match result_mode with
    | Deferred ->
      t.open_queries <- t.open_queries - 1;
      if t.open_queries = 0 then Bvar.broadcast t.no_open_queries_event ()
    | Blocking -> ()
  ;;

  let apply_implementation
    t
    implementation
    ~impl_menu_index
    ~(query : Nat0.t P.Query.Validated.t)
    ~read_buffer
    ~read_buffer_pos_ref
    ~close_connection_monitor
    ~on_exception
    ~message_bytes_for_tracing
    : _ Transport.Handler_result.t
    =
    let id = query.id in
    let rpc : Description.t =
      { name = P.Rpc_tag.to_string query.tag; version = query.version }
    in
    let emit_regular_query_tracing_event () =
      write_tracing_event
        t
        { event = Received Query
        ; rpc
        ; id = (query.id :> Int63.t)
        ; payload_bytes = message_bytes_for_tracing
        }
    in
    match implementation with
    | Implementation.F.One_way (bin_query_reader, f, here) ->
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
           (* We use [Eager_deferred] so in the non-blocking cases this will still run
              synchronously. *)
           let d =
             Rpc_result.try_with
               ~here
               (fun () ->
                 Eager_deferred.map (f t.connection_state q) ~f:(fun result -> Ok result))
               ~on_background_exception:
                 (On_exception.to_background_monitor_rest
                    on_exception
                    rpc
                    ~close_connection_monitor)
               ~location:"server-side one-way rpc computation"
           in
           match Eager_deferred.peek d with
           | None ->
             (* [Continue] on async [One_way]. Background exceptions are handled by
                [Rpc_result ~on_background_exception] *)
             Continue
           | Some result ->
             (match result with
              | Ok (_ : unit Or_not_authorized.t Or_error.t) ->
                (* We don't close connections on unauthorized RPCs. Since there is no way
                   to communicate failure back to the sender in one-way rpcs, we do
                   nothing. *)
                Continue
              | Error (Uncaught_exn sexp) ->
                (match
                   On_exception.handle_exn_before_implementation_returns
                     on_exception
                     (Exn.create_s sexp)
                     rpc
                     ~close_connection_monitor
                 with
                 | `Stop -> Stop (Error (Rpc_error.Uncaught_exn sexp))
                 | `Continue -> Continue)
              | Error error -> Stop (Error error))
         in
         write_tracing_event
           t
           { event = Sent (Response One_way_so_no_response)
           ; rpc
           ; id :> Int63.t
           ; payload_bytes = 0
           };
         result)
    | Implementation.F.One_way_expert f ->
      emit_regular_query_tracing_event ();
      let result : _ Transport_intf.Handler_result.t =
        try
          let len = (query.data :> int) in
          let (_ : _ Or_not_authorized.t Or_error.t Deferred.t) =
            f t.connection_state read_buffer ~pos:!read_buffer_pos_ref ~len
          in
          read_buffer_pos_ref := !read_buffer_pos_ref + len;
          Continue
        with
        | exn ->
          (match
             On_exception.handle_exn_before_implementation_returns
               on_exception
               exn
               rpc
               ~close_connection_monitor
           with
           | `Stop ->
             Stop
               (Rpc_result.uncaught_exn
                  exn
                  ~location:"server-side one-way rpc expert computation")
           | `Continue -> Continue)
      in
      write_tracing_event
        t
        { event = Sent (Response One_way_so_no_response)
        ; rpc
        ; id :> Int63.t
        ; payload_bytes = 0
        };
      result
    | Implementation.F.Rpc
        (bin_query_reader, bin_response_writer, f, error_mode, result_mode, here) ->
      emit_regular_query_tracing_event ();
      increment_open_queries t result_mode;
      let query_contents =
        bin_read_from_bigstring
          bin_query_reader
          read_buffer
          ~pos_ref:read_buffer_pos_ref
          ~len:query.data
          ~location:"server-side rpc query un-bin-io'ing"
      in
      let lift_result_to_rpc_result = function
        | Ok (Or_not_authorized.Authorized result) -> Ok result
        | Ok (Not_authorized error) -> authorization_failure_result error ~rpc_kind:"rpc"
        | Error error -> lift_failure_result error ~rpc_kind:"rpc"
      in
      (match result_mode with
       | Implementation_mode.Result_mode.Blocking ->
         (match
            Result.bind query_contents ~f:(fun query ->
              f t.connection_state query |> lift_result_to_rpc_result)
          with
          | response ->
            write_single_response
              t
              id
              impl_menu_index
              bin_response_writer
              response
              ~rpc
              ~error_mode
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
              impl_menu_index
              bin_response_writer
              (Error (Rpc_error.Uncaught_exn sexp))
              ~rpc
              ~error_mode;
            let (`Stop | `Continue) =
              On_exception.handle_exn_before_implementation_returns
                on_exception
                exn
                rpc
                ~close_connection_monitor
            in
            ())
       | Implementation_mode.Result_mode.Deferred ->
         let result =
           (* We generally try to write a response before handling [on_exception] so if we
              are closing the connection we still actually send the response back. When we
              pass [on_exception] here, we are making it possible for raised exceptions
              not to be written back to the client (e.g. if the implementation raises both
              asynchronously and synchronously). This would be hard to handle in a more
              principled way. *)
           Rpc_result.try_with
             ~here
             ~location:"server-side rpc computation"
             ~on_background_exception:
               (On_exception.to_background_monitor_rest
                  on_exception
                  rpc
                  ~close_connection_monitor)
             (fun () ->
                match query_contents with
                | Error err -> Deferred.return (Error err)
                | Ok query ->
                  Eager_deferred.map (f t.connection_state query) ~f:(fun result ->
                    Ok result))
         in
         let handle_result result =
           decrement_open_queries t result_mode;
           let write_response response =
             write_single_response
               t
               id
               impl_menu_index
               bin_response_writer
               response
               ~rpc
               ~error_mode [@nontail]
           in
           match result with
           | Error error as result ->
             write_response result;
             maybe_dispatch_on_exception error on_exception rpc ~close_connection_monitor
           | Ok (Ok (Or_not_authorized.Authorized result)) -> write_response (Ok result)
           | Ok (Ok (Not_authorized error)) ->
             write_response (authorization_failure_result error ~rpc_kind:"rpc")
           | Ok (Error error) ->
             write_response (lift_failure_result error ~rpc_kind:"rpc")
         in
         (* In the common case that the implementation returns a value immediately, we
            will write the response immediately as well (this is also why the above
            [try_with] has [~run:`Now]). This can be a big performance win for servers
            that get many queries in a single Async cycle. *)
         (match Deferred.peek result with
          | None -> result >>> handle_result
          | Some result -> handle_result result));
      Continue
    | Implementation.F.Menu_rpc menu ->
      emit_regular_query_tracing_event ();
      let query_contents =
        bin_read_from_bigstring
          Menu.Stable.V1.bin_reader_query
          read_buffer
          ~pos_ref:read_buffer_pos_ref
          ~len:query.data
          ~location:"server-side rpc query un-bin-io'ing"
      in
      let error_mode = Implementation_mode.Error_mode.Always_ok in
      (try
         Result.map query_contents ~f:(fun () ->
           (* We have to map down to the V1 menu since that's the type that old clients
              are expecting (but we need to be able to self-dispatch the menu to get the
              V3 response for connection metadata). *)
           force menu |> Menu.supported_rpcs |> Menu.Stable.V1.response_of_model)
         |> write_single_response
              t
              id
              impl_menu_index
              Menu.Stable.V1.bin_writer_response
              ~rpc
              ~error_mode
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
           impl_menu_index
           Menu.Stable.V1.bin_writer_response
           (Error (Rpc_error.Uncaught_exn sexp))
           ~rpc
           ~error_mode;
         let (`Stop | `Continue) =
           On_exception.handle_exn_before_implementation_returns
             on_exception
             exn
             rpc
             ~close_connection_monitor
         in
         ());
      Continue
    | Implementation.F.Rpc_expert (f, result_mode) ->
      emit_regular_query_tracing_event ();
      increment_open_queries t result_mode;
      let responder =
        Implementation.Expert.Responder.create query.id impl_menu_index t.writer
      in
      let deferred =
        (* We need the [Monitor.try_with] even for the blocking mode as the implementation
           might return [Delayed_reponse], so we don't bother optimizing the blocking
           mode. *)
        Monitor.try_with_local
          ?rest:
            (On_exception.to_background_monitor_rest
               on_exception
               rpc
               ~close_connection_monitor)
          (fun () ->
             let len = (query.data :> int) in
             let result =
               f t.connection_state responder read_buffer ~pos:!read_buffer_pos_ref ~len
             in
             match result_mode with
             | Implementation_mode.Result_mode.Deferred -> result
             | Blocking -> Deferred.return result)
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
              impl_menu_index
              bin_writer_unit
              result
              ~rpc
              ~error_mode:Always_ok;
            Ok ())
        in
        result
      in
      let check_responded () =
        if responder.responded
        then (
          write_tracing_event
            t
            { event = Sent (Response Expert_single_succeeded_or_failed)
            ; rpc
            ; id :> Int63.t
            ; payload_bytes = 0
            };
          Ok ())
        else
          handle_exn
            (computation_failure_result (Failure "Expert implementation did not reply"))
      in
      let deferred =
        let open Eager_deferred.Let_syntax in
        match%map deferred with
        | Ok (Ok (Authorized result)) ->
          let deferred =
            match result with
            | Replied -> Deferred.unit
            | Delayed_response deferred -> deferred
          in
          if Deferred.is_determined deferred
          then check_responded ()
          else (
            upon deferred (fun () ->
              check_responded ()
              |> Rpc_result.or_error
                   ~rpc_description:rpc
                   ~connection_description:t.connection_description
                   ~connection_close_started:t.connection_close_started_info
              |> ok_exn);
            Ok ())
        | Ok (Ok (Not_authorized error)) ->
          handle_exn (authorization_failure_result error ~rpc_kind:"rpc expert")
        | Ok (Error error) ->
          handle_exn (lift_failure_result error ~rpc_kind:"rpc expert")
        | Error exn ->
          let result = handle_exn (computation_failure_result exn) in
          let (`Stop | `Continue) =
            On_exception.handle_exn_before_implementation_returns
              on_exception
              exn
              rpc
              ~close_connection_monitor
          in
          result
      in
      (match Deferred.peek deferred with
       | None ->
         Wait
           (let%map result = deferred in
            decrement_open_queries t result_mode;
            ok_exn
              (Rpc_result.or_error
                 ~rpc_description:rpc
                 ~connection_description:t.connection_description
                 ~connection_close_started:t.connection_close_started_info
                 result))
       | Some result ->
         decrement_open_queries t result_mode;
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
         write_response'
           t
           id
           impl_menu_index
           [%bin_writer: Nothing.t]
           error
           ~ok_kind:Single_or_streaming_rpc_error_or_exn
           ~rpc
           ~error_mode:Always_ok
       | Ok `Abort ->
         write_tracing_event
           t
           { event = Received Abort_streaming_rpc_query
           ; rpc
           ; id = (query.id :> Int63.t)
           ; payload_bytes = message_bytes_for_tracing
           };
         (* Note that there's some delay between when we receive a pipe RPC query and when
            we put something in [open_streaming_responses] (we wait for a user-supplied
            function to return). During this time, an abort message would just be ignored.
            The dispatcher can't abort the query while this is happening, though, since
            the interface doesn't expose the ID required to abort the query until after a
            response has been returned. *)
         Option.iter (Hashtbl.find t.open_streaming_responses query.id) ~f:(function
           | Pipe pipe_placeholder -> Set_once.iter pipe_placeholder ~f:Pipe.close_read
           | Direct writer -> Direct_stream_writer.close writer)
       | Ok (`Query len) ->
         emit_regular_query_tracing_event ();
         apply_streaming_implementation
           t
           streaming_rpc
           ~len
           ~read_buffer
           ~read_buffer_pos_ref
           ~id
           ~impl_menu_index
           ~rpc
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
        | Pipe pipe_placeholder ->
          (match Set_once.get pipe_placeholder with
           | None -> acc
           | Some pipe -> Deferred.ignore_m (Pipe.upstream_flushed pipe) :: acc))
    in
    Deferred.all_unit producers_flushed
  ;;

  let open_queries (T t) = t.open_queries

  let stop (T t) =
    t.stopped <- true;
    (* We collect the writers manually so that it's safe to modify
       [open_streaming_responses] in the iteration. In the probably common case where
       there are no direct stream writers, this won't allocate. *)
    let direct_stream_writers =
      Hashtbl.fold
        t.open_streaming_responses
        ~init:[]
        ~f:(fun ~key:(_ : P.Query_id.t) ~data acc ->
          match data with
          | Pipe _ -> acc
          | Direct writer ->
            (fun () ->
              writer.instance_stopped <- true;
              Direct_stream_writer.close writer)
            :: acc)
    in
    (* Clear before closing the stream writers so that the redundant hashtbl remove
       operation is cheap. *)
    Hashtbl.clear t.open_streaming_responses;
    List.iter direct_stream_writers ~f:(fun f -> f ())
  ;;

  let handle_unknown_rpc on_unknown_rpc error t { Description.name; version }
    : _ Transport.Handler_result.t
    =
    match on_unknown_rpc with
    | `Continue -> Continue
    | `Raise -> Rpc_error.raise error t.connection_description
    | `Close_connection -> Stop (Ok ())
    | `Call f ->
      (match f t.connection_state ~rpc_tag:name ~version with
       | `Close_connection -> Stop (Ok ())
       | `Continue -> Continue)
  ;;

  let handle_query_internal
    t
    ~(query : Nat0.t P.Query.Validated.t)
    ~read_buffer
    ~read_buffer_pos_ref
    ~close_connection_monitor
    ~message_bytes_for_tracing
    =
    let { implementations; on_unknown_rpc; on_exception } = t.implementations in
    let description : Description.t =
      { name = P.Rpc_tag.to_string query.tag; version = query.version }
    in
    match t.last_dispatched_implementation with
    | Some (last_desc, implementation, impl_menu_index)
      when Description.equal last_desc description ->
      apply_implementation
        t
        implementation.f
        ~impl_menu_index
        ~query
        ~read_buffer
        ~read_buffer_pos_ref
        ~close_connection_monitor
        ~on_exception:(Option.value implementation.on_exception ~default:on_exception)
        ~message_bytes_for_tracing
    | None | Some _ ->
      let impl_menu_index =
        match t.menu with
        | None -> P.Impl_menu_index.none
        | Some menu ->
          (match Menu.index menu description with
           | None -> P.Impl_menu_index.none
           | Some index -> P.Impl_menu_index.some (Nat0.of_int_exn index))
      in
      (match Hashtbl.find implementations description with
       | Some implementation ->
         t.last_dispatched_implementation
         <- Some (description, implementation, impl_menu_index);
         apply_implementation
           t
           implementation.f
           ~impl_menu_index
           ~on_exception:(Option.value implementation.on_exception ~default:on_exception)
           ~query
           ~read_buffer
           ~read_buffer_pos_ref
           ~close_connection_monitor
           ~message_bytes_for_tracing
       | None ->
         write_tracing_event
           t
           { event = Received Query
           ; rpc = description
           ; id = (query.id :> Int63.t)
           ; payload_bytes = message_bytes_for_tracing
           };
         (match on_unknown_rpc with
          | `Expert impl ->
            let { P.Query.Validated.id; metadata; data = len; tag; version } = query in
            let rpc_tag = P.Rpc_tag.to_string tag in
            let d =
              let responder = Responder.create id impl_menu_index t.writer in
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
            write_tracing_event
              t
              { event = Sent (Response Expert_single_succeeded_or_failed)
              ; rpc = { name = rpc_tag; version = description.version }
              ; id :> Int63.t
              ; payload_bytes = 0
              };
            if Deferred.is_determined d then Continue else Wait d
          | (`Continue | `Raise | `Close_connection | `Call _) as on_unknown_rpc ->
            let error = Rpc_error.Unimplemented_rpc (query.tag, `Version query.version) in
            write_single_response
              t
              query.id
              impl_menu_index
              P.Message.bin_writer_nat0_t
              (Error error)
              ~rpc:description
              ~error_mode:Always_ok;
            handle_unknown_rpc on_unknown_rpc error t description))
  ;;

  let handle_query
    (T t)
    ~(query : Nat0.t P.Query.Validated.t)
    ~read_buffer
    ~read_buffer_pos_ref
    ~close_connection_monitor
    ~message_bytes_for_tracing
    =
    if t.stopped || Protocol_writer.is_closed t.writer
    then Transport.Handler_result.Stop (Ok ())
    else (
      let description =
        { Description.name = P.Rpc_tag.to_string query.tag; version = query.version }
      in
      let old_ctx = Async_kernel_scheduler.current_execution_context () in
      let new_ctx = t.on_receive description ~query_id:query.id query.metadata old_ctx in
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

let create ~implementations:i's ~on_unknown_rpc ~on_exception =
  (* Make sure the tags are unique. *)
  let implementations = Description.Table.create ~size:(List.length i's) () in
  let dups = Description.Hash_set.create () in
  List.iter i's ~f:(fun (i : _ Implementation.t) ->
    let description =
      { Description.name = P.Rpc_tag.to_string i.tag; version = i.version }
    in
    match Hashtbl.add implementations ~key:description ~data:i with
    | `Ok -> ()
    | `Duplicate -> Hash_set.add dups description);
  if not (Hash_set.is_empty dups)
  then Error (`Duplicate_implementations (Hash_set.to_list dups))
  else
    Ok { implementations; on_unknown_rpc :> _ on_unknown_rpc_with_expert; on_exception }
;;

let instantiate
  t
  ~menu
  ~connection_description
  ~connection_close_started
  ~connection_state
  ~writer
  ~tracing_events
  ~on_receive
  ~no_open_queries_event
  : Instance.t
  =
  T
    { implementations = t
    ; menu
    ; writer
    ; tracing_events
    ; no_open_queries_event
    ; open_streaming_responses = Hashtbl.Poly.create ()
    ; open_queries = 0
    ; connection_state
    ; connection_description
    ; connection_close_started
    ; connection_close_started_info =
        Deferred.map connection_close_started ~f:Close_reason.info_of_t
    ; stopped = false
    ; last_dispatched_implementation = None
    ; on_receive
    }
;;

exception Duplicate_implementations of Description.t list
[@@deriving sexp ~nonportable__magic_unsafe_in_parallel_programs]

let create_exn ~implementations ~on_unknown_rpc ~on_exception =
  match create ~implementations ~on_unknown_rpc ~on_exception with
  | Ok x -> x
  | Error (`Duplicate_implementations dups) -> raise (Duplicate_implementations dups)
;;

let null () =
  create_exn
    ~implementations:[]
    ~on_unknown_rpc:`Raise
    ~on_exception:Log_on_background_exn
;;

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

let lift { implementations; on_unknown_rpc; on_exception } ~f =
  let implementations = Hashtbl.map implementations ~f:(Implementation.lift ~f) in
  let on_unknown_rpc =
    match on_unknown_rpc with
    | (`Raise | `Continue | `Close_connection) as x -> x
    | `Call call -> `Call (fun state -> call (f state))
    | `Expert expert -> `Expert (fun state -> expert (f state))
  in
  { implementations; on_unknown_rpc; on_exception }
;;

let map_implementations { implementations; on_unknown_rpc; on_exception } ~f =
  create ~implementations:(f (Hashtbl.data implementations)) ~on_unknown_rpc ~on_exception
;;

module Expert = struct
  module Responder = Responder

  module Rpc_responder = struct
    type t = Responder.t

    let cannot_send ~(here : [%call_pos]) r =
      failwiths ~here "Message cannot be sent" r [%sexp_of: unit Transport.Send_result.t]
    ;;

    let mark_responded ~(here : [%call_pos]) (t : t) =
      if t.responded then failwiths ~here "Already responded" t [%sexp_of: Responder.t];
      t.responded <- true
    ;;

    let schedule (t : t) buf ~pos ~len =
      mark_responded t;
      match
        Protocol_writer.Response.send_expert
          t.writer
          t.query_id
          t.impl_menu_index
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

    let handle_send_result : local_ unit Transport.Send_result.t -> unit = function
      | Sent { result = (); bytes = (_ : int) } | Closed -> ()
      | Message_too_big _ as r ->
        cannot_send ([%globalize: unit Transport.Send_result.t] r)
    ;;

    let write_bigstring (t : t) buf ~pos ~len =
      mark_responded t;
      Protocol_writer.Response.send_expert
        t.writer
        t.query_id
        t.impl_menu_index
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
      Protocol_writer.Response.send
        t.writer
        t.query_id
        t.impl_menu_index
        ~data
        ~bin_writer_response:Nothing.bin_writer_t
      |> handle_send_result;
      ()
    ;;

    let write_bin_prot (t : t) bin_writer_a a =
      mark_responded t;
      Protocol_writer.Response.send
        t.writer
        t.query_id
        t.impl_menu_index
        ~data:(Ok a)
        ~bin_writer_response:bin_writer_a
      |> handle_send_result;
      ()
    ;;
  end

  let create_exn = create_exn
end

module Private = struct
  let to_implementation_list (t : _ t) =
    Hashtbl.data t.implementations, t.on_unknown_rpc, t.on_exception
  ;;
end
