open! Core
open! Async
open! Import

let rpc ~on_exception =
  let rpc =
    Async_rpc_kernel.Rpc.One_way.create
      ~name:"one-way"
      ~version:1
      ~bin_msg:[%bin_type_class: string]
  in
  let response1 = Ivar.create () in
  let response2 = Ivar.create () in
  let responses = Queue.of_list [ response1; response2 ] in
  let impl =
    Async_rpc_kernel.Rpc.One_way.implement ~on_exception rpc (fun (_ : Mock_peer.t) q ->
      print_s [%message "Implementation_called" q];
      Eager_deferred.upon (Ivar.read (Queue.dequeue_exn responses)) Result.ok_exn)
  in
  let plain_rpc =
    Async_rpc_kernel.Rpc.Rpc.implement
      (Async_rpc_kernel.Rpc.Rpc.create
         ~name:"plain"
         ~version:1
         ~bin_query:[%bin_type_class: int]
         ~bin_response:[%bin_type_class: int]
         ~include_in_error_count:Only_on_exn)
      (fun (_ : Mock_peer.t) q ->
        print_s [%message "Implementation_called" [%string "plain %{Int.to_string q}"]];
        Deferred.never ())
  in
  let impl =
    Async_rpc_kernel.Rpc.Implementations.create_exn
      ~implementations:[ impl; plain_rpc ]
      ~on_unknown_rpc:`Raise
      ~on_exception:Log_on_background_exn
  in
  response1, response2, impl
;;

let default_config : Mock_peer.Config.t =
  { when_reader_waits = `Carry_on_until_end_of_batch; when_waiting_done = `Read_more }
;;

let two_rpc_test ~on_exception =
  let rpc_response1, rpc_response2, implementations = rpc ~on_exception in
  let%map t = Mock_peer.create_and_connect' ~implementations default_config in
  t, rpc_response1, rpc_response2
;;

let write_query ?don't_read_yet ?(id = 100) t =
  Mock_peer.write_message
    ?don't_read_yet
    t
    [%bin_writer: string]
    (Query_v3
       { tag = Protocol.Rpc_tag.of_string "one-way"
       ; version = 1
       ; id = Protocol.Query_id.of_int_exn id
       ; metadata = None
       ; data = [%string "example message (id = %{Int.to_string id})"]
       })
;;

let write_plain_query t =
  Mock_peer.write_message
    ~don't_read_yet:()
    t
    [%bin_writer: int]
    (Query_v3
       { tag = Protocol.Rpc_tag.of_string "plain"
       ; version = 1
       ; id = Protocol.Query_id.of_int_exn 99
       ; metadata = None
       ; data = -1
       })
;;

let%expect_test "Single one-way rpc call" =
  let%bind t, rpc_response1, (_ : _ Ivar.t) =
    two_rpc_test ~on_exception:Close_connection
  in
  write_query t;
  [%expect
    {|
    (Tracing_event
     ((event (Received Query)) (rpc ((name one-way) (version 1))) (id 100)
      (payload_bytes 40)))
    (Implementation_called "example message (id = 100)")
    (Tracing_event
     ((event (Sent (Response One_way_so_no_response)))
      (rpc ((name one-way) (version 1))) (id 100) (payload_bytes 0)))
    |}];
  Ivar.fill_exn rpc_response1 (Ok ());
  let%bind () = Scheduler.yield_until_no_jobs_remain () in
  [%expect {| |}];
  return ()
;;

let%expect_test "One-way immediately raises with on_exception:Close_connection" =
  let%bind t, response1, response2 = two_rpc_test ~on_exception:Close_connection in
  Ivar.fill_exn response1 (Error (Failure "injected error"));
  Ivar.fill_exn response2 (Ok ());
  (* The point of the plain query is to show that nothing responds to it when the
     connection is closed. *)
  Dynamic.set_root Backtrace.elide true;
  write_plain_query t;
  write_query t ~id:100 ~don't_read_yet:();
  write_query t ~id:200;
  let%bind () = Scheduler.yield_until_no_jobs_remain () in
  Dynamic.set_root Backtrace.elide false;
  [%expect
    {|
    (Tracing_event
     ((event (Received Query)) (rpc ((name plain) (version 1))) (id 99)
      (payload_bytes 13)))
    (Implementation_called "plain -1")
    (Tracing_event
     ((event (Received Query)) (rpc ((name one-way) (version 1))) (id 100)
      (payload_bytes 40)))
    (Implementation_called "example message (id = 100)")
    (Tracing_event
     ((event (Sent (Response One_way_so_no_response)))
      (rpc ((name one-way) (version 1))) (id 100) (payload_bytes 0)))
    (Send
     (message
      ("00000000  0b 00 0b 55 6e 73 70 65  63 69 66 69 65 64 01 07  |...Unspecified..|"
       "00000010  00 02 01 21 52 70 63 20  6d 65 73 73 61 67 65 20  |...!Rpc message |"
       "00000020  68 61 6e 64 6c 69 6e 67  20 6c 6f 6f 70 20 73 74  |handling loop st|"
       "00000030  6f 70 70 65 64 03 01 02  00 0c 55 6e 63 61 75 67  |opped.....Uncaug|"
       "00000040  68 74 5f 65 78 6e 01 02  01 02 00 08 6c 6f 63 61  |ht_exn......loca|"
       "00000050  74 69 6f 6e 00 23 73 65  72 76 65 72 2d 73 69 64  |tion.#server-sid|"
       "00000060  65 20 6f 6e 65 2d 77 61  79 20 72 70 63 20 63 6f  |e one-way rpc co|"
       "00000070  6d 70 75 74 61 74 69 6f  6e 01 02 00 03 65 78 6e  |mputation....exn|"
       "00000080  01 03 00 10 6d 6f 6e 69  74 6f 72 2e 6d 6c 2e 45  |....monitor.ml.E|"
       "00000090  72 72 6f 72 01 02 00 07  46 61 69 6c 75 72 65 00  |rror....Failure.|"
       "000000a0  0e 69 6e 6a 65 63 74 65  64 20 65 72 72 6f 72 01  |.injected error.|"
       "000000b0  01 00 1a 3c 62 61 63 6b  74 72 61 63 65 20 65 6c  |...<backtrace el|"
       "000000c0  69 64 65 64 20 69 6e 20  74 65 73 74 3e 00        |ided in test>.|")))
    (Close_started
     ("Rpc message handling loop stopped"
      (Uncaught_exn
       ((location "server-side one-way rpc computation")
        (exn
         (monitor.ml.Error (Failure "injected error")
          ("<backtrace elided in test>")))))
      (connection_description <created-directly>)))
    Close_writer
    Close_reader
    Close_finished
    |}];
  return ()
;;

let%expect_test "One-way asynchronously raises with on_exception:Close_connection" =
  let%bind t, response1, response2 = two_rpc_test ~on_exception:Close_connection in
  Dynamic.set_root Backtrace.elide true;
  write_plain_query t;
  write_query t ~id:100 ~don't_read_yet:();
  write_query t ~id:200;
  Ivar.fill_exn response2 (Ok ());
  Ivar.fill_exn response1 (Error (Failure "injected error"));
  let%bind () = Scheduler.yield_until_no_jobs_remain () in
  Dynamic.set_root Backtrace.elide false;
  [%expect
    {|
    (Tracing_event
     ((event (Received Query)) (rpc ((name plain) (version 1))) (id 99)
      (payload_bytes 13)))
    (Implementation_called "plain -1")
    (Tracing_event
     ((event (Received Query)) (rpc ((name one-way) (version 1))) (id 100)
      (payload_bytes 40)))
    (Implementation_called "example message (id = 100)")
    (Tracing_event
     ((event (Sent (Response One_way_so_no_response)))
      (rpc ((name one-way) (version 1))) (id 100) (payload_bytes 0)))
    (Tracing_event
     ((event (Received Query)) (rpc ((name one-way) (version 1))) (id 200)
      (payload_bytes 42)))
    (Implementation_called "example message (id = 200)")
    (Tracing_event
     ((event (Sent (Response One_way_so_no_response)))
      (rpc ((name one-way) (version 1))) (id 200) (payload_bytes 0)))
    (Send
     (message
      ("00000000  0b 00 0b 55 6e 73 70 65  63 69 66 69 65 64 01 03  |...Unspecified..|"
       "00000010  01 02 00 24 55 6e 63 61  75 67 68 74 20 65 78 63  |...$Uncaught exc|"
       "00000020  65 70 74 69 6f 6e 20 69  6e 20 69 6d 70 6c 65 6d  |eption in implem|"
       "00000030  65 6e 74 61 74 69 6f 6e  01 02 00 03 65 78 6e 01  |entation....exn.|"
       "00000040  03 00 10 6d 6f 6e 69 74  6f 72 2e 6d 6c 2e 45 72  |...monitor.ml.Er|"
       "00000050  72 6f 72 01 02 00 07 46  61 69 6c 75 72 65 00 0e  |ror....Failure..|"
       "00000060  69 6e 6a 65 63 74 65 64  20 65 72 72 6f 72 01 01  |injected error..|"
       "00000070  00 1a 3c 62 61 63 6b 74  72 61 63 65 20 65 6c 69  |..<backtrace eli|"
       "00000080  64 65 64 20 69 6e 20 74  65 73 74 3e 00           |ded in test>.|")))
    (Close_started
     (("Connection closed by local side:"
       ("Uncaught exception in implementation"
        (exn
         (monitor.ml.Error (Failure "injected error")
          ("<backtrace elided in test>")))))
      (connection_description <created-directly>)))
    Close_writer
    Close_reader
    Close_finished
    |}];
  return ()
;;

let%expect_test "One-way immediately raises, with on_exception:Log_on_background_exn" =
  (* We expect no log since it's a foreground exception. *)
  let%bind t, response1, response2 = two_rpc_test ~on_exception:Log_on_background_exn in
  Ivar.fill_exn response1 (Error (Failure "injected error"));
  Ivar.fill_exn response2 (Ok ());
  write_plain_query t;
  write_query t ~id:100 ~don't_read_yet:();
  write_query t ~id:200;
  let%bind () = Scheduler.yield_until_no_jobs_remain () in
  [%expect
    {|
    (Tracing_event
     ((event (Received Query)) (rpc ((name plain) (version 1))) (id 99)
      (payload_bytes 13)))
    (Implementation_called "plain -1")
    (Tracing_event
     ((event (Received Query)) (rpc ((name one-way) (version 1))) (id 100)
      (payload_bytes 40)))
    (Implementation_called "example message (id = 100)")
    (Tracing_event
     ((event (Sent (Response One_way_so_no_response)))
      (rpc ((name one-way) (version 1))) (id 100) (payload_bytes 0)))
    (Tracing_event
     ((event (Received Query)) (rpc ((name one-way) (version 1))) (id 200)
      (payload_bytes 42)))
    (Implementation_called "example message (id = 200)")
    (Tracing_event
     ((event (Sent (Response One_way_so_no_response)))
      (rpc ((name one-way) (version 1))) (id 200) (payload_bytes 0)))
    |}];
  return ()
;;

let%expect_test "One-way asynchronously raises, with on_exception:Log_on_background_exn" =
  (* We expect a log since it's an asynchronous exception. *)
  let%bind t, response1, response2 = two_rpc_test ~on_exception:Log_on_background_exn in
  Dynamic.set_root Backtrace.elide true;
  write_plain_query t;
  write_query t ~don't_read_yet:() ~id:100;
  write_query t ~id:200;
  Ivar.fill_exn response2 (Ok ());
  Ivar.fill_exn response1 (Error (Failure "injected error"));
  let%bind () = Scheduler.yield_until_no_jobs_remain () in
  Dynamic.set_root Backtrace.elide false;
  [%expect
    {|
    (Tracing_event
     ((event (Received Query)) (rpc ((name plain) (version 1))) (id 99)
      (payload_bytes 13)))
    (Implementation_called "plain -1")
    (Tracing_event
     ((event (Received Query)) (rpc ((name one-way) (version 1))) (id 100)
      (payload_bytes 40)))
    (Implementation_called "example message (id = 100)")
    (Tracing_event
     ((event (Sent (Response One_way_so_no_response)))
      (rpc ((name one-way) (version 1))) (id 100) (payload_bytes 0)))
    (Tracing_event
     ((event (Received Query)) (rpc ((name one-way) (version 1))) (id 200)
      (payload_bytes 42)))
    (Implementation_called "example message (id = 200)")
    (Tracing_event
     ((event (Sent (Response One_way_so_no_response)))
      (rpc ((name one-way) (version 1))) (id 200) (payload_bytes 0)))
    1969-12-31 19:00:00.000000-05:00 Error ("Exception raised to [Monitor.try_with] that already returned.""This error was captured by a default handler in [Async.Log]."(exn(monitor.ml.Error(Failure"injected error")("<backtrace elided in test>"))))
    |}];
  return ()
;;
