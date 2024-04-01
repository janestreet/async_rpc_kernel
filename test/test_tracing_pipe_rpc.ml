open! Core
open! Async
open Import

let expect_initial_response mock =
  let module M = struct
    type t = (unit, string) Protocol.Stream_initial_message.t [@@deriving bin_io, sexp_of]
  end
  in
  Mock_peer.expect_message mock [%bin_reader: M.t] [%sexp_of: M.t]
;;

let expect_stream_query mock =
  let module M = struct
    type t = string Binio_printer_helper.With_length.t Protocol.Stream_query.needs_length
    [@@deriving bin_io, sexp_of]
  end
  in
  Mock_peer.expect_message mock [%bin_reader: M.t] [%sexp_of: M.t]
;;

let expect_pipe_message ?later mock =
  Mock_peer.expect_message
    ?later
    mock
    [%bin_reader:
      string Binio_printer_helper.With_length.t Protocol.Stream_response_data.t]
    [%sexp_of: string Protocol.Stream_response_data.t]
;;

let pipe_rpc =
  Async_rpc_kernel.Rpc.Pipe_rpc.create
    ~name:"pipe-rpc"
    ~version:1
    ~bin_query:[%bin_type_class: string]
    ~bin_response:[%bin_type_class: string]
    ~bin_error:[%bin_type_class: string]
    ()
;;

let rpc () =
  let response1 = Ivar.create () in
  let response2 = Ivar.create () in
  let responses = Queue.of_list [ response1; response2 ] in
  let impl =
    Async_rpc_kernel.Rpc.Pipe_rpc.implement pipe_rpc (fun mock q ->
      print_s [%message "Implementation_called" q];
      let response = Queue.dequeue_exn responses in
      let%map.Eager_deferred r = Ivar.read response in
      expect_initial_response mock;
      let r = Result.ok_exn r in
      r)
  in
  let impl =
    Async_rpc_kernel.Rpc.Implementations.create_exn
      ~implementations:[ impl ]
      ~on_unknown_rpc:`Raise
  in
  response1, response2, impl
;;

let write_query ?don't_read_yet ?(id = 55) t =
  let data =
    Bin_prot.Writer.to_bigstring
      [%bin_writer: string]
      [%string "example query (id = %{Int.to_string id})"]
  in
  Mock_peer.write_message
    ?don't_read_yet
    t
    [%bin_writer: Bigstring.Stable.V1.t Protocol.Stream_query.needs_length]
    (Query
       { tag = Protocol.Rpc_tag.of_string "pipe-rpc"
       ; version = 1
       ; id = Protocol.Query_id.of_int_exn id
       ; metadata = None
       ; data = `Query data
       })
;;

let write_abort ?(id = 55) t =
  Mock_peer.write_message
    t
    [%bin_writer: Nothing.t Protocol.Stream_query.needs_length]
    (Query
       { tag = Protocol.Rpc_tag.of_string "pipe-rpc"
       ; version = 1
       ; id = Protocol.Query_id.of_int_exn id
       ; metadata = None
       ; data = `Abort
       })
;;

let default_config : Mock_peer.Config.t =
  { when_reader_waits = `Carry_on_until_end_of_batch; when_waiting_done = `Read_more }
;;

let single_rpc_test () =
  let rpc_response, _rpc_response2, implementations = rpc () in
  let%map t = Mock_peer.create_and_connect' ~implementations default_config in
  t, rpc_response
;;

let direct_rpc_test () =
  let writer_iv = Ivar.create () in
  let response = Ivar.create () in
  let impl =
    Async_rpc_kernel.Rpc.Pipe_rpc.implement_direct pipe_rpc (fun mock q writer ->
      print_s [%message "Implementation_called" q];
      Ivar.fill_exn writer_iv writer;
      let%map.Eager_deferred r = Ivar.read response in
      expect_initial_response mock;
      let r = Result.ok_exn r in
      r)
  in
  let implementations =
    Async_rpc_kernel.Rpc.Implementations.create_exn
      ~implementations:[ impl ]
      ~on_unknown_rpc:`Raise
  in
  let%map t = Mock_peer.create_and_connect' ~implementations default_config in
  t, Ivar.read writer_iv, response
;;

let%expect_test "Single pipe-rpc implementation returning error" =
  let%bind t, rpc_response = single_rpc_test () in
  write_query t;
  [%expect
    {|
    (Tracing_event
     ((event (Received Query)) (rpc (((name pipe-rpc) (version 1)))) (id 55)
      (payload_bytes 43)))
    |}];
  let%bind () = Scheduler.yield_until_no_jobs_remain () in
  [%expect {| (Implementation_called "example query (id = 55)") |}];
  Ivar.fill_exn rpc_response (Ok (Error "example error"));
  let%bind () = Scheduler.yield_until_no_jobs_remain () in
  [%expect
    {|
    (Send
     (Response
      ((id 55)
       (data (Ok ((unused_query_id 0) (initial (Error "example error"))))))))
    (Tracing_event
     ((event (Sent (Response Single_or_streaming_user_defined_error)))
      (rpc (((name pipe-rpc) (version 1)))) (id 55) (payload_bytes 1)))
    |}];
  let%bind () = Scheduler.yield_until_no_jobs_remain () in
  return ()
;;

let%expect_test "Single pipe-rpc implementation raising" =
  let%bind t, rpc_response = single_rpc_test () in
  write_query t;
  [%expect
    {|
    (Tracing_event
     ((event (Received Query)) (rpc (((name pipe-rpc) (version 1)))) (id 55)
      (payload_bytes 43)))
    |}];
  let%bind () = Scheduler.yield_until_no_jobs_remain () in
  [%expect {| (Implementation_called "example query (id = 55)") |}];
  Backtrace.elide := true;
  Ivar.fill_exn rpc_response (Error (Failure "injected error"));
  let%bind () = Scheduler.yield_until_no_jobs_remain () in
  Backtrace.elide := false;
  [%expect
    {|
    (Send
     (Response
      ((id 55)
       (data
        (Error
         (Uncaught_exn
          ((location "server-side pipe_rpc computation")
           (exn
            (monitor.ml.Error (Failure "injected error")
             ("<backtrace elided in test>"))))))))))
    (Tracing_event
     ((event (Sent (Response Single_or_streaming_rpc_error_or_exn)))
      (rpc (((name pipe-rpc) (version 1)))) (id 55) (payload_bytes 1)))
    |}];
  let%bind () = Scheduler.yield_until_no_jobs_remain () in
  return ()
;;

let%expect_test "Single pipe-rpc implementation succeeds" =
  let%bind t, rpc_response = single_rpc_test () in
  write_query t;
  [%expect
    {|
    (Tracing_event
     ((event (Received Query)) (rpc (((name pipe-rpc) (version 1)))) (id 55)
      (payload_bytes 43)))
    |}];
  let%bind () = Scheduler.yield_until_no_jobs_remain () in
  [%expect {| (Implementation_called "example query (id = 55)") |}];
  let r, w = Pipe.create () in
  Ivar.fill_exn rpc_response (Ok (Ok r));
  let%bind () = Scheduler.yield_until_no_jobs_remain () in
  [%expect
    {|
    (Send
     (Response ((id 55) (data (Ok ((unused_query_id 0) (initial (Ok ()))))))))
    (Tracing_event
     ((event (Sent (Response Streaming_initial)))
      (rpc (((name pipe-rpc) (version 1)))) (id 55) (payload_bytes 1)))
    |}];
  expect_pipe_message t;
  Pipe.write_without_pushback w "foo";
  let%bind () = Scheduler.yield_until_no_jobs_remain () in
  [%expect
    {|
    (Send (Response ((id 55) (data (Ok (Ok foo))))))
    (Tracing_event
     ((event (Sent (Response Streaming_update)))
      (rpc (((name pipe-rpc) (version 1)))) (id 55) (payload_bytes 1)))
    Wait_for_writer_ready
    |}];
  expect_pipe_message t;
  Pipe.close w;
  let%bind () = Pipe.closed w in
  let%bind () = Scheduler.yield_until_no_jobs_remain () in
  [%expect {| (Wait_for_flushed 0) |}];
  Mock_peer.mark_flushed_up_to t 0;
  let%bind () = Scheduler.yield_until_no_jobs_remain () in
  [%expect
    {|
    (Send (Response ((id 55) (data (Ok Eof)))))
    (Tracing_event
     ((event (Sent (Response Streaming_closed)))
      (rpc (((name pipe-rpc) (version 1)))) (id 55) (payload_bytes 1)))
    |}];
  let%bind () = Scheduler.yield_until_no_jobs_remain () in
  return ()
;;

let%expect_test "Malformed query for pipe-rpc" =
  let%bind t, rpc_response = single_rpc_test () in
  Ivar.fill_exn rpc_response (Ok (Error "error-if-impl-ever-called"));
  expect_initial_response t;
  Mock_peer.write_message
    t
    [%bin_writer: string]
    (Query
       { tag = Protocol.Rpc_tag.of_string "pipe-rpc"
       ; version = 1
       ; id = Protocol.Query_id.of_int_exn 99
       ; metadata = None
       ; data = "malformed"
       });
  [%expect
    {|
    (Tracing_event
     ((event (Received Query)) (rpc (((name pipe-rpc) (version 1)))) (id 99)
      (payload_bytes 24)))
    (Send
     (Response
      ((id 99)
       (data
        (Error
         (Bin_io_exn
          ((location "server-side pipe_rpc stream_query un-bin-io'ing")
           (exn
            (common.ml.Read_error
             "Variant / protocol.ml.Stream_query.needs_length" 18)))))))))
    (Tracing_event
     ((event (Sent (Response Single_or_streaming_rpc_error_or_exn)))
      (rpc (((name pipe-rpc) (version 1)))) (id 99) (payload_bytes 1)))
    |}];
  let%bind () = Scheduler.yield_until_no_jobs_remain () in
  [%expect {| |}];
  return ()
;;

let%expect_test "direct stream writer impl raises synchronously" =
  let%bind t, writer, response = direct_rpc_test () in
  Ivar.fill_exn response (Error (Failure "injected error"));
  Backtrace.elide := true;
  write_query t;
  let%bind () = Scheduler.yield_until_no_jobs_remain () in
  Backtrace.elide := false;
  [%expect
    {|
    (Tracing_event
     ((event (Received Query)) (rpc (((name pipe-rpc) (version 1)))) (id 55)
      (payload_bytes 43)))
    (Implementation_called "example query (id = 55)")
    (Send
     (Response
      ((id 55)
       (data
        (Error
         (Uncaught_exn
          ((location "server-side pipe_rpc computation")
           (exn
            (monitor.ml.Error (Failure "injected error")
             ("<backtrace elided in test>"))))))))))
    (Tracing_event
     ((event (Sent (Response Single_or_streaming_rpc_error_or_exn)))
      (rpc (((name pipe-rpc) (version 1)))) (id 55) (payload_bytes 1)))
    |}];
  let%bind writer = writer in
  print_s
    ([%sexp_of: bool]
       (Async_rpc_kernel.Rpc.Pipe_rpc.Direct_stream_writer.is_closed writer));
  [%expect {| true |}];
  let write = Async_rpc_kernel.Rpc.Pipe_rpc.Direct_stream_writer.write writer "example" in
  print_s ([%sexp_of: [ `Closed | `Flushed of unit Deferred.t ]] write);
  [%expect {| Closed |}];
  return ()
;;

let%expect_test "direct stream writer impl raises asynchronously" =
  let%bind t, writer, response = direct_rpc_test () in
  write_query t;
  let%bind () = Scheduler.yield_until_no_jobs_remain () in
  [%expect
    {|
    (Tracing_event
     ((event (Received Query)) (rpc (((name pipe-rpc) (version 1)))) (id 55)
      (payload_bytes 43)))
    (Implementation_called "example query (id = 55)")
    |}];
  let%bind writer = writer in
  let write1 =
    Async_rpc_kernel.Rpc.Pipe_rpc.Direct_stream_writer.write writer "example1"
  in
  Backtrace.elide := true;
  Ivar.fill_exn response (Error (Failure "injected error"));
  let%bind () = Scheduler.yield_until_no_jobs_remain () in
  Backtrace.elide := false;
  [%expect
    {|
    (Wait_for_flushed 0)
    (Send
     (Response
      ((id 55)
       (data
        (Error
         (Uncaught_exn
          ((location "server-side pipe_rpc computation")
           (exn
            (monitor.ml.Error (Failure "injected error")
             ("<backtrace elided in test>"))))))))))
    (Tracing_event
     ((event (Sent (Response Single_or_streaming_rpc_error_or_exn)))
      (rpc (((name pipe-rpc) (version 1)))) (id 55) (payload_bytes 1)))
    |}];
  print_s
    ([%sexp_of: bool]
       (Async_rpc_kernel.Rpc.Pipe_rpc.Direct_stream_writer.is_closed writer));
  [%expect {| true |}];
  Mock_peer.mark_flushed_up_to t 0;
  let%bind () = Scheduler.yield_until_no_jobs_remain () in
  let write2 =
    Async_rpc_kernel.Rpc.Pipe_rpc.Direct_stream_writer.write writer "example2"
  in
  print_s ([%sexp_of: [ `Closed | `Flushed of unit Deferred.t ] list] [ write1; write2 ]);
  [%expect {| ((Flushed (Full ())) Closed) |}];
  return ()
;;

let%expect_test "direct stream writer returns error" =
  let%bind t, writer, response = direct_rpc_test () in
  Ivar.fill_exn response (Ok (Error "error"));
  Backtrace.elide := true;
  write_query t;
  let%bind () = Scheduler.yield_until_no_jobs_remain () in
  Backtrace.elide := false;
  [%expect
    {|
    (Tracing_event
     ((event (Received Query)) (rpc (((name pipe-rpc) (version 1)))) (id 55)
      (payload_bytes 43)))
    (Implementation_called "example query (id = 55)")
    (Send
     (Response
      ((id 55) (data (Ok ((unused_query_id 0) (initial (Error error))))))))
    (Tracing_event
     ((event (Sent (Response Single_or_streaming_user_defined_error)))
      (rpc (((name pipe-rpc) (version 1)))) (id 55) (payload_bytes 1)))
    |}];
  let%bind writer = writer in
  print_s
    ([%sexp_of: bool]
       (Async_rpc_kernel.Rpc.Pipe_rpc.Direct_stream_writer.is_closed writer));
  [%expect {| true |}];
  let write = Async_rpc_kernel.Rpc.Pipe_rpc.Direct_stream_writer.write writer "example" in
  print_s ([%sexp_of: [ `Closed | `Flushed of unit Deferred.t ]] write);
  [%expect {| Closed |}];
  return ()
;;

let%expect_test "direct stream writer" =
  let%bind t, writer, response = direct_rpc_test () in
  write_query t;
  let%bind () = Scheduler.yield_until_no_jobs_remain () in
  [%expect
    {|
    (Tracing_event
     ((event (Received Query)) (rpc (((name pipe-rpc) (version 1)))) (id 55)
      (payload_bytes 43)))
    (Implementation_called "example query (id = 55)")
    |}];
  let%bind writer = writer in
  print_s
    ([%sexp_of: bool]
       (Async_rpc_kernel.Rpc.Pipe_rpc.Direct_stream_writer.is_closed writer));
  [%expect {| false |}];
  let do_three_writes key =
    let open Async_rpc_kernel.Rpc.Pipe_rpc.Direct_stream_writer in
    (match write writer "write" with
     | `Closed -> print_endline "write -> closed"
     | `Flushed d -> upon d (fun () -> printf "write flushed: %s\n" key));
    let buf =
      Bin_prot.Writer.to_bigstring [%bin_writer: string] "write_without_pushback"
    in
    (match
       Expert.write_without_pushback writer ~buf ~pos:0 ~len:(Bigstring.length buf)
     with
     | `Closed -> print_endline "write_without_pushback -> closed"
     | `Ok -> print_endline "write_without_pushback -> ok");
    let buf = Bin_prot.Writer.to_bigstring [%bin_writer: string] "schedule_write" in
    match Expert.schedule_write writer ~buf ~pos:0 ~len:(Bigstring.length buf) with
    | `Closed -> print_endline "schedule_write -> closed"
    | `Flushed { g = d } -> upon d (fun () -> printf "schedule_write flushed: %s\n" key)
  in
  let expect_three_writes ?later () =
    expect_pipe_message ?later t;
    expect_pipe_message ?later t;
    expect_pipe_message ?later t
  in
  do_three_writes "batch_before_response";
  [%expect {|
    (Wait_for_flushed 0)
    write_without_pushback -> ok
    |}];
  Mock_peer.mark_flushed_up_to t 0;
  let%bind () = Scheduler.yield_until_no_jobs_remain () in
  [%expect {| write flushed: batch_before_response |}];
  let writes_finished = Bvar.create () in
  Mock_peer.scheduled_writes_must_wait_for t (Bvar.wait writes_finished);
  Ivar.fill_exn response (Ok (Ok ()));
  expect_three_writes ~later:() ();
  let%bind () = Scheduler.yield_until_no_jobs_remain () in
  [%expect
    {|
    (Send
     (Response ((id 55) (data (Ok ((unused_query_id 0) (initial (Ok ()))))))))
    (Tracing_event
     ((event (Sent (Response Streaming_initial)))
      (rpc (((name pipe-rpc) (version 1)))) (id 55) (payload_bytes 1)))
    (Send (Response ((id 55) (data (Ok (Ok write))))))
    (Tracing_event
     ((event (Sent (Response Streaming_update)))
      (rpc (((name pipe-rpc) (version 1)))) (id 55) (payload_bytes 1)))
    (Send (Response ((id 55) (data (Ok (Ok write_without_pushback))))))
    (Tracing_event
     ((event (Sent (Response Streaming_update)))
      (rpc (((name pipe-rpc) (version 1)))) (id 55) (payload_bytes 1)))
    (Send_with_bigstring_non_copying
     (Response ((id 55) (data (Ok (Ok schedule_write))))))
    Wait_for_writer_ready
    (Tracing_event
     ((event (Sent (Response Streaming_update)))
      (rpc (((name pipe-rpc) (version 1)))) (id 55) (payload_bytes 1)))
    |}];
  Bvar.broadcast writes_finished ();
  let%bind () = Scheduler.yield_until_no_jobs_remain () in
  [%expect {| schedule_write flushed: batch_before_response |}];
  Mock_peer.scheduled_writes_must_wait_for t (Bvar.wait writes_finished);
  expect_three_writes ();
  do_three_writes "batch_after_response";
  [%expect
    {|
    (Send (Response ((id 55) (data (Ok (Ok write))))))
    (Tracing_event
     ((event (Sent (Response Streaming_update)))
      (rpc (((name pipe-rpc) (version 1)))) (id 55) (payload_bytes 1)))
    (Wait_for_flushed 1)
    (Send_with_bigstring
     (Response ((id 55) (data (Ok (Ok write_without_pushback))))))
    (Tracing_event
     ((event (Sent (Response Streaming_update)))
      (rpc (((name pipe-rpc) (version 1)))) (id 55) (payload_bytes 1)))
    write_without_pushback -> ok
    (Send_with_bigstring_non_copying
     (Response ((id 55) (data (Ok (Ok schedule_write))))))
    Wait_for_writer_ready
    (Tracing_event
     ((event (Sent (Response Streaming_update)))
      (rpc (((name pipe-rpc) (version 1)))) (id 55) (payload_bytes 1)))
    |}];
  Bvar.broadcast writes_finished ();
  let%bind () = Scheduler.yield_until_no_jobs_remain () in
  [%expect {| schedule_write flushed: batch_after_response |}];
  Mock_peer.mark_flushed_up_to t 1;
  let%bind () = Scheduler.yield_until_no_jobs_remain () in
  [%expect {| write flushed: batch_after_response |}];
  expect_pipe_message t;
  write_abort t;
  let%bind () = Scheduler.yield_until_no_jobs_remain () in
  [%expect
    {|
    (Tracing_event
     ((event (Received Abort_streaming_rpc_query))
      (rpc (((name pipe-rpc) (version 1)))) (id 55) (payload_bytes 18)))
    (Send (Response ((id 55) (data (Ok Eof)))))
    (Tracing_event
     ((event (Sent (Response Streaming_closed)))
      (rpc (((name pipe-rpc) (version 1)))) (id 55) (payload_bytes 1)))
    |}];
  print_s
    ([%sexp_of: bool]
       (Async_rpc_kernel.Rpc.Pipe_rpc.Direct_stream_writer.is_closed writer));
  [%expect {| true |}];
  do_three_writes "after_abort";
  [%expect
    {|
    write -> closed
    write_without_pushback -> closed
    schedule_write -> closed
    |}];
  return ()
;;

let%expect_test "direct stream writer errors after a scheduled write" =
  let%bind t, writer, response = direct_rpc_test () in
  write_query t;
  let%bind () = Scheduler.yield_until_no_jobs_remain () in
  [%expect
    {|
    (Tracing_event
     ((event (Received Query)) (rpc (((name pipe-rpc) (version 1)))) (id 55)
      (payload_bytes 43)))
    (Implementation_called "example query (id = 55)")
    |}];
  let%bind writer = writer in
  let buf = Bin_prot.Writer.to_bigstring [%bin_writer: string] "example" in
  let schedule_result =
    Async_rpc_kernel.Rpc.Pipe_rpc.Direct_stream_writer.Expert.schedule_write
      writer
      ~buf
      ~pos:0
      ~len:(Bigstring.length buf)
  in
  let can_reuse_bigstring =
    match schedule_result with
    | `Closed ->
      print_endline "closed";
      None
    | `Flushed { g = d } -> Some d
  in
  Ivar.fill_exn response (Ok (Error "error"));
  let%bind () = Scheduler.yield_until_no_jobs_remain () in
  [%expect
    {|
    (Send
     (Response
      ((id 55) (data (Ok ((unused_query_id 0) (initial (Error error))))))))
    (Tracing_event
     ((event (Sent (Response Single_or_streaming_user_defined_error)))
      (rpc (((name pipe-rpc) (version 1)))) (id 55) (payload_bytes 1)))
    |}];
  print_s ([%sexp_of: unit Deferred.t option] can_reuse_bigstring);
  [%expect {| ((Full ())) |}];
  return ()
;;

let%expect_test "direct stream writer connection closed after a scheduled write, before \
                 initial response"
  =
  let%bind t, writer, response = direct_rpc_test () in
  write_query t;
  let%bind () = Scheduler.yield_until_no_jobs_remain () in
  [%expect
    {|
    (Tracing_event
     ((event (Received Query)) (rpc (((name pipe-rpc) (version 1)))) (id 55)
      (payload_bytes 43)))
    (Implementation_called "example query (id = 55)")
    |}];
  let%bind writer = writer in
  let buf = Bin_prot.Writer.to_bigstring [%bin_writer: string] "example" in
  let schedule_result =
    Async_rpc_kernel.Rpc.Pipe_rpc.Direct_stream_writer.Expert.schedule_write
      writer
      ~buf
      ~pos:0
      ~len:(Bigstring.length buf)
  in
  let can_reuse_bigstring =
    match schedule_result with
    | `Closed ->
      print_endline "closed";
      None
    | `Flushed { g = d } -> Some d
  in
  for _ = 1 to 10 do
    Mock_peer.enqueue_send_result t Closed
  done;
  Ivar.fill_exn response (Ok (Error "error"));
  let%bind () = Scheduler.yield_until_no_jobs_remain () in
  [%expect
    {|
    (Send
     (Response
      ((id 55) (data (Ok ((unused_query_id 0) (initial (Error error))))))))
    (Tracing_event
     ((event
       (Failed_to_send (Response Single_or_streaming_user_defined_error) Closed))
      (rpc (((name pipe-rpc) (version 1)))) (id 55) (payload_bytes 0)))
    |}];
  print_s ([%sexp_of: unit Deferred.t option] can_reuse_bigstring);
  [%expect {| ((Full ())) |}];
  return ()
;;

let%expect_test "error dispatching a pipe rpc" =
  let%bind t, conn = Mock_peer.create_and_connect default_config in
  expect_stream_query t;
  let result = Async_rpc_kernel.Rpc.Pipe_rpc.dispatch' pipe_rpc conn "query" in
  [%expect
    {|
    (Send
     (Query
      ((tag pipe-rpc) (version 1) (id 1) (metadata ()) (data (Query query)))))
    (Tracing_event
     ((event (Sent Query)) (rpc (((name pipe-rpc) (version 1)))) (id 1)
      (payload_bytes 1)))
    |}];
  Mock_peer.write_message
    t
    [%bin_writer: (unit, string) Protocol.Stream_initial_message.t]
    (Response
       { id = Protocol.Query_id.of_int_exn 1
       ; data = Error (Uncaught_exn (Atom "injected error"))
       });
  let%bind () = Scheduler.yield_until_no_jobs_remain () in
  [%expect
    {|
    (Tracing_event
     ((event
       (Received
        (Response
         (Response_finished_rpc_error_or_exn (Uncaught_exn "injected error")))))
      (rpc ()) (id 1) (payload_bytes 20)))
    |}];
  let%bind result = result in
  print_s ([%sexp_of: ((_, string) Result.t, Protocol.Rpc_error.t) Result.t] result);
  [%expect {| (Error (Uncaught_exn "injected error")) |}];
  return ()
;;

let%expect_test "initial error response when dispatching a pipe rpc" =
  let%bind t, conn = Mock_peer.create_and_connect default_config in
  expect_stream_query t;
  let result = Async_rpc_kernel.Rpc.Pipe_rpc.dispatch' pipe_rpc conn "query" in
  [%expect
    {|
    (Send
     (Query
      ((tag pipe-rpc) (version 1) (id 1) (metadata ()) (data (Query query)))))
    (Tracing_event
     ((event (Sent Query)) (rpc (((name pipe-rpc) (version 1)))) (id 1)
      (payload_bytes 1)))
    |}];
  Mock_peer.write_message
    t
    [%bin_writer: (unit, string) Protocol.Stream_initial_message.t]
    (Response
       { id = Protocol.Query_id.of_int_exn 1
       ; data =
           Ok
             { unused_query_id = Protocol.Unused_query_id.t; initial = Error "rpc error" }
       });
  let%bind () = Scheduler.yield_until_no_jobs_remain () in
  [%expect
    {|
    (Tracing_event
     ((event (Received (Response Response_finished_user_defined_error)))
      (rpc ()) (id 1) (payload_bytes 16)))
    |}];
  let%bind result = result in
  print_s ([%sexp_of: ((_, string) Result.t, Protocol.Rpc_error.t) Result.t] result);
  [%expect {| (Ok (Error "rpc error")) |}];
  return ()
;;

let%expect_test "calling pipe_rpc expecting a regular rpc" =
  let%bind t, conn = Mock_peer.create_and_connect default_config in
  let regular =
    Async_rpc_kernel.Rpc.Rpc.create
      ~name:"rpc"
      ~version:1
      ~bin_query:[%bin_type_class: string]
      ~bin_response:[%bin_type_class: unit]
      ~include_in_error_count:Only_on_exn
  in
  Mock_peer.expect_message t [%bin_reader: string] [%sexp_of: string];
  let result = Async_rpc_kernel.Rpc.Rpc.dispatch' regular conn "query" in
  [%expect
    {|
    (Send (Query ((tag rpc) (version 1) (id 1) (metadata ()) (data query))))
    (Tracing_event
     ((event (Sent Query)) (rpc (((name rpc) (version 1)))) (id 1)
      (payload_bytes 1)))
    |}];
  Mock_peer.write_message
    t
    [%bin_writer: (unit, string) Protocol.Stream_initial_message.t]
    (Response
       { id = Protocol.Query_id.of_int_exn 1
       ; data = Error (Bin_io_exn (Atom "binio error reading query"))
       });
  let%bind () = Scheduler.yield_until_no_jobs_remain () in
  [%expect
    {|
    (Tracing_event
     ((event
       (Received
        (Response
         (Response_finished_rpc_error_or_exn
          (Bin_io_exn "binio error reading query")))))
      (rpc ()) (id 1) (payload_bytes 31)))
    |}];
  let%bind result = result in
  print_s ([%sexp_of: unit Protocol.Rpc_result.t] result);
  [%expect {| (Error (Bin_io_exn "binio error reading query")) |}];
  return ()
;;

let%expect_test "calling pipe_rpc expecting a one-way rpc" =
  let%bind t, conn = Mock_peer.create_and_connect default_config in
  let regular =
    Async_rpc_kernel.Rpc.One_way.create
      ~name:"rpc"
      ~version:1
      ~bin_msg:[%bin_type_class: string]
  in
  Mock_peer.expect_message t [%bin_reader: string] [%sexp_of: string];
  let result = Async_rpc_kernel.Rpc.One_way.dispatch' regular conn "query" in
  [%expect
    {|
    (Send (Query ((tag rpc) (version 1) (id 1) (metadata ()) (data query))))
    (Tracing_event
     ((event (Sent Query)) (rpc (((name rpc) (version 1)))) (id 1)
      (payload_bytes 1)))
    (Tracing_event
     ((event (Received (Response One_way_so_no_response)))
      (rpc (((name rpc) (version 1)))) (id 1) (payload_bytes 1)))
    |}];
  print_s ([%sexp_of: unit Protocol.Rpc_result.t] result);
  [%expect {| (Ok ()) |}];
  Mock_peer.write_message
    t
    [%bin_writer: (unit, string) Protocol.Stream_initial_message.t]
    (Response
       { id = Protocol.Query_id.of_int_exn 1
       ; data = Error (Bin_io_exn (Atom "binio error reading query"))
       });
  let%bind () = Scheduler.yield_until_no_jobs_remain () in
  [%expect
    {|
    (Close_started ("Rpc message handling loop stopped" (Unknown_query_id 1)))
    Close_writer
    Close_reader
    Close_finished
    |}];
  return ()
;;

let%expect_test "attempt to abort a pipe-rpc and server returns an Rpc_error" =
  let%bind t, conn = Mock_peer.create_and_connect default_config in
  expect_stream_query t;
  let result = Async_rpc_kernel.Rpc.Pipe_rpc.dispatch_exn pipe_rpc conn "query" in
  [%expect
    {|
    (Send
     (Query
      ((tag pipe-rpc) (version 1) (id 1) (metadata ()) (data (Query query)))))
    (Tracing_event
     ((event (Sent Query)) (rpc (((name pipe-rpc) (version 1)))) (id 1)
      (payload_bytes 1)))
    |}];
  Mock_peer.write_message
    t
    [%bin_writer: (unit, string) Protocol.Stream_initial_message.t]
    (Response
       { id = Protocol.Query_id.of_int_exn 1
       ; data = Ok { unused_query_id = Protocol.Unused_query_id.t; initial = Ok () }
       });
  let%bind () = Scheduler.yield_until_no_jobs_remain () in
  [%expect
    {|
    (Tracing_event
     ((event (Received (Response Partial_response))) (rpc ()) (id 1)
      (payload_bytes 7)))
    |}];
  let%bind pipe, _metadata = result in
  expect_stream_query t;
  Pipe.close_read pipe;
  let%bind () = Scheduler.yield_until_no_jobs_remain () in
  [%expect
    {|
    (Send (Query ((tag pipe-rpc) (version 1) (id 1) (metadata ()) (data Abort))))
    (Tracing_event
     ((event (Sent Abort_streaming_rpc_query))
      (rpc (((name pipe-rpc) (version 1)))) (id 1) (payload_bytes 1)))
    (Tracing_event
     ((event (Received (Response One_way_so_no_response)))
      (rpc (((name pipe-rpc) (version 1)))) (id 1) (payload_bytes 1)))
    |}];
  Mock_peer.write_message
    t
    [%bin_writer: (unit, string) Protocol.Stream_initial_message.t]
    (Response
       { id = Protocol.Query_id.of_int_exn 1
       ; data = Error (Bin_io_exn (Atom "binio error reading query"))
       });
  let%bind () = Scheduler.yield_until_no_jobs_remain () in
  [%expect
    {|
    (Tracing_event
     ((event
       (Received
        (Response
         (Response_finished_rpc_error_or_exn
          (Bin_io_exn "binio error reading query")))))
      (rpc ()) (id 1) (payload_bytes 31)))
    (Close_started
     ("Rpc message handling loop stopped"
      (Bin_io_exn "binio error reading query")))
    Close_writer
    Close_reader
    Close_finished
    |}];
  return ()
;;

let%expect_test "Client sends abort after server closes pipe" =
  let%bind t, rpc_response = single_rpc_test () in
  let id = 1234 in
  write_query ~id t;
  [%expect
    {|
    (Tracing_event
     ((event (Received Query)) (rpc (((name pipe-rpc) (version 1)))) (id 1234)
      (payload_bytes 47)))
    |}];
  let%bind () = Scheduler.yield_until_no_jobs_remain () in
  [%expect {| (Implementation_called "example query (id = 1234)") |}];
  Ivar.fill_exn rpc_response (Ok (Ok (Pipe.of_list [])));
  let%bind () = Scheduler.yield_until_no_jobs_remain () in
  [%expect
    {|
    (Send
     (Response ((id 1234) (data (Ok ((unused_query_id 0) (initial (Ok ()))))))))
    (Tracing_event
     ((event (Sent (Response Streaming_initial)))
      (rpc (((name pipe-rpc) (version 1)))) (id 1234) (payload_bytes 1)))
    (Wait_for_flushed 0)
    |}];
  Mock_peer.mark_flushed_up_to t 0;
  expect_pipe_message t;
  let%bind () = Scheduler.yield_until_no_jobs_remain () in
  [%expect
    {|
    (Send (Response ((id 1234) (data (Ok Eof)))))
    (Tracing_event
     ((event (Sent (Response Streaming_closed)))
      (rpc (((name pipe-rpc) (version 1)))) (id 1234) (payload_bytes 1)))
    |}];
  write_abort ~id t;
  let%bind () = Scheduler.yield_until_no_jobs_remain () in
  [%expect
    {|
    (Tracing_event
     ((event (Received Abort_streaming_rpc_query))
      (rpc (((name pipe-rpc) (version 1)))) (id 1234) (payload_bytes 20)))
    |}];
  return ()
;;
