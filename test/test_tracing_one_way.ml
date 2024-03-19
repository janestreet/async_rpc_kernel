open! Core
open! Async
open! Import

let rpc ?on_exception () =
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
    Async_rpc_kernel.Rpc.One_way.implement ?on_exception rpc (fun (_ : Mock_peer.t) q ->
      print_s [%message "Implementation_called" q];
      Eager_deferred.upon
        (Ivar.read (Queue.dequeue_exn responses))
        (fun r -> Result.ok_exn r))
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
  in
  response1, response2, impl
;;

let default_config : Mock_peer.Config.t =
  { when_reader_waits = `Carry_on_until_end_of_batch; when_waiting_done = `Read_more }
;;

let two_rpc_test ?on_exception () =
  let rpc_response1, rpc_response2, implementations = rpc ?on_exception () in
  let%map t = Mock_peer.create_and_connect' ~implementations default_config in
  t, rpc_response1, rpc_response2
;;

let write_query ?don't_read_yet ?(id = 100) t =
  Mock_peer.write_message
    ?don't_read_yet
    t
    [%bin_writer: string]
    (Query
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
    (Query
       { tag = Protocol.Rpc_tag.of_string "plain"
       ; version = 1
       ; id = Protocol.Query_id.of_int_exn 99
       ; metadata = None
       ; data = -1
       })
;;

let%expect_test "Single one-way rpc call" =
  let%bind t, rpc_response1, (_ : _ Ivar.t) = two_rpc_test () in
  write_query t;
  [%expect
    {|
    (Tracing_event
     ((event (Received Query)) (rpc (((name one-way) (version 1)))) (id 100)
      (payload_bytes 40)))
    (Implementation_called "example message (id = 100)")
    (Tracing_event
     ((event (Sent (Response One_way_so_no_response)))
      (rpc (((name one-way) (version 1)))) (id 100) (payload_bytes 0)))
    |}];
  Ivar.fill_exn rpc_response1 (Ok ());
  let%bind () = Scheduler.yield_until_no_jobs_remain () in
  [%expect {| |}];
  return ()
;;

let%expect_test "One-way immediately raises" =
  let%bind t, response1, response2 = two_rpc_test () in
  Ivar.fill_exn response1 (Error (Failure "injected error"));
  Ivar.fill_exn response2 (Ok ());
  (* The point of the plain query is to show that nothing responds to it when the
     connection is closed. *)
  write_plain_query t;
  write_query t ~id:100 ~don't_read_yet:();
  write_query t ~id:200;
  let%bind () = Scheduler.yield_until_no_jobs_remain () in
  [%expect
    {|
    (Tracing_event
     ((event (Received Query)) (rpc (((name plain) (version 1)))) (id 99)
      (payload_bytes 13)))
    (Implementation_called "plain -1")
    (Tracing_event
     ((event (Received Query)) (rpc (((name one-way) (version 1)))) (id 100)
      (payload_bytes 40)))
    (Implementation_called "example message (id = 100)")
    (Tracing_event
     ((event (Sent (Response One_way_so_no_response)))
      (rpc (((name one-way) (version 1)))) (id 100) (payload_bytes 0)))
    (Close_started
     ("Rpc message handling loop stopped"
      (Uncaught_exn
       ((location "server-side one-way rpc computation")
        (exn (Failure "injected error"))))))
    Close_writer
    Close_reader
    Close_finished
    |}];
  return ()
;;

let%expect_test "One-way asynchronously raises" =
  let%bind t, response1, response2 = two_rpc_test () in
  write_plain_query t;
  write_query t ~id:100 ~don't_read_yet:();
  write_query t ~id:200;
  Ivar.fill_exn response2 (Ok ());
  Ivar.fill_exn response1 (Error (Failure "injected error"));
  let%bind () = Scheduler.yield_until_no_jobs_remain () in
  [%expect
    {|
    (Tracing_event
     ((event (Received Query)) (rpc (((name plain) (version 1)))) (id 99)
      (payload_bytes 13)))
    (Implementation_called "plain -1")
    (Tracing_event
     ((event (Received Query)) (rpc (((name one-way) (version 1)))) (id 100)
      (payload_bytes 40)))
    (Implementation_called "example message (id = 100)")
    (Tracing_event
     ((event (Sent (Response One_way_so_no_response)))
      (rpc (((name one-way) (version 1)))) (id 100) (payload_bytes 0)))
    (Tracing_event
     ((event (Received Query)) (rpc (((name one-way) (version 1)))) (id 200)
      (payload_bytes 42)))
    (Implementation_called "example message (id = 200)")
    (Tracing_event
     ((event (Sent (Response One_way_so_no_response)))
      (rpc (((name one-way) (version 1)))) (id 200) (payload_bytes 0)))
    (Close_started
     ("exn raised in RPC connection loop"
      (monitor.ml.Error (Failure "injected error")
       ("<backtrace elided in test>" "Caught by monitor RPC connection loop"))))
    Close_writer
    Close_reader
    Close_finished
    |}];
  return ()
;;

let%expect_test "One-way immediately raises, with on_exception:continue" =
  let%bind t, response1, response2 =
    two_rpc_test () ~on_exception:Async_rpc_kernel.Rpc.On_exception.continue
  in
  Ivar.fill_exn response1 (Error (Failure "injected error"));
  Ivar.fill_exn response2 (Ok ());
  write_plain_query t;
  write_query t ~id:100 ~don't_read_yet:();
  write_query t ~id:200;
  let%bind () = Scheduler.yield_until_no_jobs_remain () in
  [%expect
    {|
    (Tracing_event
     ((event (Received Query)) (rpc (((name plain) (version 1)))) (id 99)
      (payload_bytes 13)))
    (Implementation_called "plain -1")
    (Tracing_event
     ((event (Received Query)) (rpc (((name one-way) (version 1)))) (id 100)
      (payload_bytes 40)))
    (Implementation_called "example message (id = 100)")
    (Tracing_event
     ((event (Sent (Response One_way_so_no_response)))
      (rpc (((name one-way) (version 1)))) (id 100) (payload_bytes 0)))
    (Tracing_event
     ((event (Received Query)) (rpc (((name one-way) (version 1)))) (id 200)
      (payload_bytes 42)))
    (Implementation_called "example message (id = 200)")
    (Tracing_event
     ((event (Sent (Response One_way_so_no_response)))
      (rpc (((name one-way) (version 1)))) (id 200) (payload_bytes 0)))
    |}];
  return ()
;;

let%expect_test "One-way asynchronously raises, with on_exception:continue" =
  let%bind t, response1, response2 =
    two_rpc_test () ~on_exception:Async_rpc_kernel.Rpc.On_exception.continue
  in
  write_plain_query t;
  write_query t ~don't_read_yet:() ~id:100;
  write_query t ~id:200;
  Ivar.fill_exn response2 (Ok ());
  Ivar.fill_exn response1 (Error (Failure "injected error"));
  let%bind () = Scheduler.yield_until_no_jobs_remain () in
  [%expect
    {|
    (Tracing_event
     ((event (Received Query)) (rpc (((name plain) (version 1)))) (id 99)
      (payload_bytes 13)))
    (Implementation_called "plain -1")
    (Tracing_event
     ((event (Received Query)) (rpc (((name one-way) (version 1)))) (id 100)
      (payload_bytes 40)))
    (Implementation_called "example message (id = 100)")
    (Tracing_event
     ((event (Sent (Response One_way_so_no_response)))
      (rpc (((name one-way) (version 1)))) (id 100) (payload_bytes 0)))
    (Tracing_event
     ((event (Received Query)) (rpc (((name one-way) (version 1)))) (id 200)
      (payload_bytes 42)))
    (Implementation_called "example message (id = 200)")
    (Tracing_event
     ((event (Sent (Response One_way_so_no_response)))
      (rpc (((name one-way) (version 1)))) (id 200) (payload_bytes 0)))
    (Close_started
     ("exn raised in RPC connection loop"
      (monitor.ml.Error (Failure "injected error")
       ("<backtrace elided in test>" "Caught by monitor RPC connection loop"))))
    Close_writer
    Close_reader
    Close_finished
    |}];
  return ()
;;
