open! Core
open! Async
open Import

let expect_initial_response mock =
  let module M = struct
    type t = (string, string) Protocol.Stream_initial_message.t
    [@@deriving bin_io, sexp_of]
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

let default_config : Mock_peer.Config.t =
  { when_reader_waits = `Carry_on_until_end_of_batch; when_waiting_done = `Read_more }
;;

let single_rpc_test () =
  let rpc_response, _rpc_response2, implementations = rpc () in
  let%map t = Mock_peer.create_and_connect' ~implementations default_config in
  t, rpc_response
;;

let%expect_test "Single pipe-rpc implementation returning error" =
  let%bind t, rpc_response = single_rpc_test () in
  write_query t;
  [%expect
    {|
    (Tracing_event
     ((event (Received Query)) (rpc (((name pipe-rpc) (version 1)))) (id 55)
      (payload_bytes 43))) |}];
  let%bind () = Scheduler.yield_until_no_jobs_remain () in
  [%expect {|
    (Implementation_called "example query (id = 55)") |}];
  Ivar.fill_exn rpc_response (Ok (Error "example error"));
  let%bind () = Scheduler.yield_until_no_jobs_remain () in
  [%expect
    {|
    (Send
     (Response
      ((id 55)
       (data (Ok ((unused_query_id 0) (initial (Error "example error"))))))))
    (Tracing_event
     ((event (Sent (Response Single_or_streaming_error)))
      (rpc (((name pipe-rpc) (version 1)))) (id 55) (payload_bytes 1))) |}];
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
      (payload_bytes 43))) |}];
  let%bind () = Scheduler.yield_until_no_jobs_remain () in
  [%expect {|
    (Implementation_called "example query (id = 55)") |}];
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
     ((event (Sent (Response Single_or_streaming_error)))
      (rpc (((name pipe-rpc) (version 1)))) (id 55) (payload_bytes 1))) |}];
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
      (payload_bytes 43))) |}];
  let%bind () = Scheduler.yield_until_no_jobs_remain () in
  [%expect {|
    (Implementation_called "example query (id = 55)") |}];
  let r, w = Pipe.create () in
  Ivar.fill_exn rpc_response (Ok (Ok r));
  let%bind () = Scheduler.yield_until_no_jobs_remain () in
  [%expect
    {|
    (Send
     (Response ((id 55) (data (Ok ((unused_query_id 0) (initial (Ok ""))))))))
    (Tracing_event
     ((event (Sent (Response Streaming_initial)))
      (rpc (((name pipe-rpc) (version 1)))) (id 55) (payload_bytes 1))) |}];
  Mock_peer.expect_message
    t
    [%bin_reader:
      string Binio_printer_helper.With_length.t Protocol.Stream_response_data.t]
    [%sexp_of: string Protocol.Stream_response_data.t];
  Pipe.write_without_pushback w "foo";
  let%bind () = Scheduler.yield_until_no_jobs_remain () in
  [%expect
    {|
    (Send (Response ((id 55) (data (Ok (Ok foo))))))
    (Tracing_event
     ((event (Sent (Response Streaming_update)))
      (rpc (((name pipe-rpc) (version 1)))) (id 55) (payload_bytes 1)))
    Wait_for_writer_ready |}];
  Mock_peer.expect_message
    t
    [%bin_reader:
      string Binio_printer_helper.With_length.t Protocol.Stream_response_data.t]
    [%sexp_of: string Protocol.Stream_response_data.t];
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
      (rpc (((name pipe-rpc) (version 1)))) (id 55) (payload_bytes 1))) |}];
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
     ((event (Sent (Response Single_or_streaming_error)))
      (rpc (((name pipe-rpc) (version 1)))) (id 99) (payload_bytes 1))) |}];
  let%bind () = Scheduler.yield_until_no_jobs_remain () in
  [%expect {| |}];
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
      (payload_bytes 1))) |}];
  [%expect {| |}];
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
     ((event (Received (Response Response_finished))) (rpc ()) (id 1)
      (payload_bytes 20))) |}];
  let%bind result = result in
  print_s ([%sexp_of: ((_, string) Result.t, Protocol.Rpc_error.t) Result.t] result);
  [%expect {|
    (Error (Uncaught_exn "injected error")) |}];
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
  in
  Mock_peer.expect_message t [%bin_reader: string] [%sexp_of: string];
  let result = Async_rpc_kernel.Rpc.Rpc.dispatch' regular conn "query" in
  [%expect
    {|
    (Send (Query ((tag rpc) (version 1) (id 1) (metadata ()) (data query))))
    (Tracing_event
     ((event (Sent Query)) (rpc (((name rpc) (version 1)))) (id 1)
      (payload_bytes 1))) |}];
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
     ((event (Received (Response Response_finished))) (rpc ()) (id 1)
      (payload_bytes 31))) |}];
  let%bind result = result in
  print_s ([%sexp_of: unit Protocol.Rpc_result.t] result);
  [%expect {|
    (Error (Bin_io_exn "binio error reading query")) |}];
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
      (payload_bytes 1))) |}];
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
    Close_finished |}];
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
      (payload_bytes 1))) |}];
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
      (payload_bytes 7))) |}];
  let%bind pipe, _metadata = result in
  expect_stream_query t;
  Pipe.close_read pipe;
  let%bind () = Scheduler.yield_until_no_jobs_remain () in
  [%expect
    {|
    (Send (Query ((tag pipe-rpc) (version 1) (id 1) (metadata ()) (data Abort))))
    (Tracing_event
     ((event (Sent Query)) (rpc (((name pipe-rpc) (version 1)))) (id 1)
      (payload_bytes 1))) |}];
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
     ((event (Received (Response Response_finished))) (rpc ()) (id 1)
      (payload_bytes 31)))
    (Close_started
     ("Rpc message handling loop stopped"
      (Bin_io_exn "binio error reading query")))
    Close_writer
    Close_reader
    Close_finished |}];
  return ()
;;
