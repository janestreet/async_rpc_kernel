open! Core
open! Async
open! Import

module Write_event = struct
  type t =
    | Send : 'a Bin_prot.Type_class.writer * 'a -> t
    | Send_with_bigstring :
        { writer : 'a Bin_prot.Type_class.writer
        ; data : 'a
        ; buf : Bigstring.t
        ; pos : int
        ; len : int
        ; non_copying : bool
        }
        -> t

  let sexp_of_t = function
    | Send _ -> Sexp.Atom "Send"
    | Send_with_bigstring _ -> Atom "Send_with_bigstring"
  ;;

  let bigstring_written t =
    match t with
    | Send (w, x) -> Bin_prot.Writer.to_bigstring w x
    | Send_with_bigstring { writer; data; buf; pos; len; non_copying = _ } ->
      let base = Bin_prot.Writer.to_bigstring writer data in
      let out = Bigstring.create (Bigstring.length base + len) in
      Bigstring.blit ~src:base ~dst:out ~src_pos:0 ~dst_pos:0 ~len:(Bigstring.length base);
      Bigstring.blit ~src:buf ~dst:out ~src_pos:pos ~dst_pos:(Bigstring.length base) ~len;
      out
  ;;
end

module Expected_message = struct
  type t =
    | T :
        { reader : 'a Bin_prot.Type_class.reader
        ; sexp_of : 'a -> Sexp.t
        }
        -> t
end

module Event = struct
  type t =
    | Close_reader
    | Reader_waiting_on_scheduler
    | Close_writer
    | Wait_for_flushed of int
    | Wait_for_writer_ready
    | Write of Write_event.t
    | Tracing_event of Tracing_event.t
    | Close_started of Sexp.t
    | Close_finished
  [@@deriving sexp_of]
end

module Config = struct
  type t =
    { when_reader_waits : [ `Carry_on_until_end_of_batch | `Wait_immediately ]
    ; when_waiting_done : [ `Read_more | `Wait_for_mock_scheduling ]
    }
  [@@deriving sexp_of]
end

type t =
  { config : Config.t
  ; time_source : Synchronous_time_source.t
  ; mutable reader_close_started : bool
  ; reader_close_finished : unit Ivar.t
  ; messages : Bigstring.t Deque.t
  ; mutable
      read_forever_state :
      [ `None
      | `Running
      | `Waiting of unit Deferred.t
      | `Waiting_for_message of unit -> unit
      | `Waiting_for_mock_scheduling of unit -> unit
      ]
  ; writer_close_started : unit Ivar.t
  ; writer_close_finished : unit Ivar.t
  ; mutable flush_counter : int
  ; writer_flushes : (int * unit Ivar.t) Queue.t
  ; mutable writes_allowed : [ `Wait of unit Deferred.t | `Ready | `Flushed ]
  ; send_result_queue : unit Rpc.Transport.Send_result.t Queue.t
  ; mutable default_send_result : unit Rpc.Transport.Send_result.t
  ; writer_monitor : Monitor.t
  ; expected_messages : (Expected_message.t[@sexp.opaque]) Queue.t
      (* transferred to expected_messages when that queue is empty *)
  ; later_expected_messages : (Expected_message.t[@sexp.opaque]) Queue.t
  ; mutable quiet : bool
  ; mutable on_emit : t -> Event.t -> unit
  }
[@@deriving sexp_of]

let next_expected_message t =
  if Queue.is_empty t.expected_messages
  then
    Queue.drain
      t.later_expected_messages
      ~f:(fun x -> Queue.enqueue t.expected_messages x)
      ~while_:(Fn.const true);
  Queue.dequeue t.expected_messages
;;

let default_on_emit t event =
  let should_print =
    match (event : Event.t) with
    | Close_reader ->
      Ivar.fill_if_empty t.reader_close_finished ();
      true
    | Close_writer ->
      Ivar.fill_if_empty t.writer_close_finished ();
      true
    | Write ev ->
      let kind =
        match ev with
        | Send (_, _) -> "Send"
        | Send_with_bigstring
            { writer = _; data = _; buf = _; pos = _; len = _; non_copying } ->
          if non_copying then "Send_with_bigstring_non_copying" else "Send_with_bigstring"
      in
      let bs = Write_event.bigstring_written ev in
      if Bigstring.length bs = 1 && Char.( = ) '\000' (Bigstring.get bs 0)
      then (
        print_s [%message kind "Heartbeat"];
        false)
      else (
        match next_expected_message t with
        | Some (T { reader; sexp_of }) ->
          (match sexp_of (Bin_prot.Reader.of_bigstring reader bs) with
           | exception exn -> print_s [%message "Failed_to_read" kind (exn : exn)]
           | sexp -> print_s [%message kind ~_:(sexp : Sexp.t)]);
          false
        | None ->
          print_s [%message kind ~message:(bs : Bigstring.Hexdump.t)];
          false)
    | Reader_waiting_on_scheduler
    | Wait_for_flushed _
    | Wait_for_writer_ready
    | Tracing_event _
    | Close_started _
    | Close_finished -> true
  in
  if should_print then print_s ([%sexp_of: Event.t] event)
;;

let emit t event = if t.quiet then () else t.on_emit t event

module Reader : Rpc.Transport.Reader.S with type t = t = struct
  type nonrec t = t [@@deriving sexp_of]

  let close t =
    emit t Close_reader;
    t.reader_close_started <- true;
    Ivar.read t.reader_close_finished
  ;;

  let is_closed t = t.reader_close_started

  (* We don’t need to return anything interesting here *)
  let bytes_read (_ : t) = Int63.zero

  let read_forever t ~on_message ~on_end_of_batch =
    let context = Scheduler.current_execution_context () in
    let on_message buf ~pos ~len =
      match Scheduler.within_context context (fun () -> on_message buf ~pos ~len) with
      | Ok x -> x
      | Error () -> failwith "on_message raised"
    in
    let on_end_of_batch () =
      match Scheduler.within_context context (fun () -> on_end_of_batch ()) with
      | Ok x -> x
      | Error () -> failwith "on_message raised"
    in
    match t.read_forever_state with
    | `Running | `Waiting _ | `Waiting_for_message _ | `Waiting_for_mock_scheduling _ ->
      failwith "Already reading"
    | `None ->
      Deferred.create (fun res ->
        let wait_and_call to_wait ~f =
          let wait = Eager_deferred.all_unit to_wait in
          match Deferred.peek wait with
          | Some () -> f ()
          | None ->
            let wait =
              Deferred.map wait ~f:(fun () ->
                match t.config.when_waiting_done with
                | `Wait_for_mock_scheduling ->
                  t.read_forever_state <- `Waiting_for_mock_scheduling f;
                  emit t Reader_waiting_on_scheduler
                | `Read_more -> f ())
            in
            t.read_forever_state <- `Waiting wait
        in
        let stop batch =
          (* plain Async_rpc transport does not call [on_end_of_batch] *)
          List.iter batch ~f:(Deque.enqueue_front t.messages);
          t.read_forever_state <- `None
        in
        let rec handle_batch to_wait continue batch =
          match batch with
          | [] -> wait_and_call to_wait ~f:continue
          | hd :: tl ->
            (match
               (on_message hd ~pos:0 ~len:(Bigstring.length hd)
                 : _ Rpc.Transport.Handler_result.t)
             with
             | Continue -> handle_batch to_wait continue tl
             | Stop x ->
               Ivar.fill_exn res (Ok x);
               wait_and_call to_wait ~f:(fun () -> stop tl)
             | Wait d ->
               let to_wait = d :: to_wait in
               (match t.config.when_reader_waits with
                | `Carry_on_until_end_of_batch -> handle_batch to_wait continue tl
                | `Wait_immediately ->
                  wait_and_call to_wait ~f:(fun () -> handle_batch [] continue tl)))
        in
        let rec handle_next_batch () =
          t.read_forever_state <- `Running;
          if Deque.length t.messages > 0
          then (
            let batch = Deque.to_list t.messages in
            Deque.clear t.messages;
            let continue () =
              on_end_of_batch ();
              match t.config.when_waiting_done with
              | `Read_more -> handle_next_batch ()
              | `Wait_for_mock_scheduling ->
                t.read_forever_state <- `Waiting_for_mock_scheduling handle_next_batch;
                emit t Reader_waiting_on_scheduler
            in
            handle_batch [] continue batch)
          else if t.reader_close_started
          then (
            Ivar.fill_exn res (Error `Closed);
            t.read_forever_state <- `None)
          else t.read_forever_state <- `Waiting_for_message handle_next_batch
        in
        handle_next_batch ())
  ;;
end

module Writer : Rpc.Transport.Writer.S with type t = t = struct
  type nonrec t = t [@@deriving sexp_of]

  let close t =
    emit t Close_writer;
    Ivar.fill_if_empty t.writer_close_started ();
    Ivar.read t.writer_close_finished
  ;;

  let is_closed t = Ivar.is_full t.writer_close_started
  let stopped t = Ivar.read t.writer_close_started
  let monitor t = t.writer_monitor
  let bytes_to_write (_ : t) = 0
  let bytes_written (_ : t) = Int63.zero

  let flushed' t ~do_emit =
    let id = t.flush_counter in
    t.flush_counter <- id + 1;
    if do_emit then emit t (Wait_for_flushed id);
    Deferred.create (fun iv -> Queue.enqueue t.writer_flushes (id, iv))
  ;;

  let flushed t = flushed' t ~do_emit:true

  let ready_to_write t =
    emit t Wait_for_writer_ready;
    match t.writes_allowed with
    | `Ready -> return ()
    | `Flushed -> flushed' t ~do_emit:false
    | `Wait d -> d
  ;;

  let send_result t =
    match Queue.dequeue t.send_result_queue with
    | Some x -> x
    | None -> t.default_send_result
  ;;

  let send_bin_prot t writer data =
    emit t (Write (Send (writer, data)));
    send_result t
  ;;

  let send_bin_prot_and_bigstring t writer data ~buf ~pos ~len =
    emit
      t
      (Write (Send_with_bigstring { writer; data; buf; pos; len; non_copying = false }));
    send_result t
  ;;

  let send_bin_prot_and_bigstring_non_copying t writer data ~buf ~pos ~len =
    emit
      t
      (Write (Send_with_bigstring { writer; data; buf; pos; len; non_copying = true }));
    match send_result t with
    | (Closed | Message_too_big _) as x -> x
    | Sent { result = (); bytes } -> Sent { result = ready_to_write t; bytes }
  ;;
end

let transport t : Rpc.Transport.t =
  { reader = Rpc.Transport.Reader.pack (module Reader) t
  ; writer = Rpc.Transport.Writer.pack (module Writer) t
  }
;;

let create ?(time_source = Synchronous_time_source.wall_clock ()) config =
  Async_rpc_kernel.Async_rpc_kernel_private.Protocol.Query_id.(
    For_testing.reset_counter ();
    ignore (create () : t));
  { config
  ; time_source
  ; reader_close_started = false
  ; reader_close_finished = Ivar.create ()
  ; messages = Deque.create ()
  ; read_forever_state = `None
  ; writer_close_started = Ivar.create ()
  ; writer_close_finished = Ivar.create ()
  ; flush_counter = 0
  ; writer_flushes = Queue.create ()
  ; writes_allowed = `Ready
  ; send_result_queue = Queue.create ()
  ; default_send_result = Sent { result = (); bytes = 1 }
  ; writer_monitor = Monitor.create ~name:"async rpc test monitor" ()
  ; expected_messages = Queue.create ()
  ; later_expected_messages = Queue.create ()
  ; quiet = false
  ; on_emit = default_on_emit
  }
;;

let write_bigstring ?don't_read_yet t bigstring =
  Deque.enqueue_back t.messages bigstring;
  match don't_read_yet with
  | Some () -> ()
  | None ->
    (match t.read_forever_state with
     | `Waiting_for_message f -> f ()
     | `Running | `Waiting_for_mock_scheduling _ | `None | `Waiting _ -> ())
;;

let close_reader t =
  t.reader_close_started <- true;
  let rec reader_closed () =
    match t.read_forever_state with
    | `Waiting_for_message f ->
      f ();
      reader_closed ()
    | `Waiting_for_mock_scheduling f ->
      f ();
      reader_closed ()
    | `Running ->
      let%bind () = Scheduler.yield () in
      reader_closed ()
    | `Waiting d ->
      let%bind () = d in
      reader_closed ()
    | `None -> return ()
  in
  upon (reader_closed ()) (Ivar.fill_if_empty t.reader_close_finished)
;;

let write ?don't_read_yet t writer x =
  let bigstring = Bin_prot.Writer.to_bigstring writer x in
  write_bigstring ?don't_read_yet t bigstring
;;

let write_handshake t handshake =
  match handshake with
  | `v3 ->
    let header = Test_helpers.Header.v3 in
    write t [%bin_writer: Test_helpers.Header.t] header;
    write
      t
      [%bin_writer: Protocol.Message.nat0_t]
      (Metadata { identification = None; menu = None })
;;

let write_message ?don't_read_yet t writer (message : _ Protocol.Message.t) =
  let second_part = ref None in
  let length data =
    let bs = Bin_prot.Writer.to_bigstring writer data in
    second_part := Some bs;
    Bigstring.length bs |> Bin_prot.Nat0.of_int
  in
  let nat0 =
    match message with
    | (Heartbeat | Metadata _) as x -> x
    | Query_v1 x -> Query_v1 { x with data = length x.data }
    | Response { data = Error _; _ } as x -> x
    | Response ({ data = Ok data; _ } as x) -> Response { x with data = Ok (length data) }
    | Query x -> Query { x with data = length x.data }
  in
  let first_part =
    Bin_prot.Writer.to_bigstring [%bin_writer: Protocol.Message.nat0_t] nat0
  in
  let bs =
    match !second_part with
    | None -> first_part
    | Some x -> Bigstring.concat [ first_part; x ]
  in
  write_bigstring ?don't_read_yet t bs
;;

let continue_reader t =
  match t.read_forever_state with
  | `Waiting_for_mock_scheduling f -> f ()
  | `Waiting_for_message _ | `Running | `None | `Waiting _ ->
    failwith "Reader not waiting"
;;

let expect ?later t reader sexp_of =
  let q =
    match later with
    | None -> t.expected_messages
    | Some () -> t.later_expected_messages
  in
  Queue.enqueue q (T { reader; sexp_of })
;;

let expect_message ?later t reader sexp_of =
  expect
    ?later
    t
    (Protocol.Message.bin_reader_t (Binio_printer_helper.With_length.bin_reader_t reader))
    (Protocol.Message.sexp_of_t sexp_of)
;;

let connect ?implementations ?(send_handshake = Some `v3) t =
  let transport = transport t in
  let r =
    Rpc.Connection.create
      ?implementations
      transport
      ~connection_state:(fun conn ->
        let bus = Connection.events conn in
        Bus.iter_exn bus [%here] ~f:(fun event ->
          let event = [%globalize: Tracing_event.t] event in
          emit t (Tracing_event event));
        t)
      ~time_source:t.time_source
  in
  upon r (function
    | Error _ -> ()
    | Ok conn ->
      upon (Rpc.Connection.close_reason ~on_close:`started conn) (fun reason ->
        let reason =
          let old_elide = !Backtrace.elide in
          Backtrace.elide := true;
          let r = [%sexp_of: Info.t] reason in
          Backtrace.elide := old_elide;
          r
        in
        emit t (Close_started reason));
      upon (Rpc.Connection.close_finished conn) (fun () -> emit t Close_finished));
  (match send_handshake with
   | None -> ()
   | Some handshake -> write_handshake t handshake);
  r
;;

let create_and_connect ?implementations config =
  let t = create config in
  t.quiet <- true;
  let%bind conn = connect ?implementations t >>| Result.ok_exn in
  let%map () = Scheduler.yield_until_no_jobs_remain () in
  t.quiet <- false;
  t, conn
;;

let create_and_connect' ?implementations config =
  let%map t, (_ : Rpc.Connection.t) = create_and_connect ?implementations config in
  t
;;

let enqueue_send_result t r = Queue.enqueue t.send_result_queue r

let mark_flushed_up_to t n =
  Queue.drain
    t.writer_flushes
    ~while_:(fun (i, (_ : unit Ivar.t)) -> i <= n)
    ~f:(fun ((_ : int), iv) -> Ivar.fill_exn iv ())
;;

let scheduled_writes_must_wait_for t deferred =
  t.writes_allowed <- `Wait deferred;
  upon deferred (fun () ->
    match t.writes_allowed with
    | `Wait d when phys_equal d deferred -> t.writes_allowed <- `Ready
    | _ -> ())
;;

let set_quiet t q = t.quiet <- q
let set_on_emit t f = t.on_emit <- (fun (_ : t) event -> f event)
