open! Core
open! Async
open Import

let rpc' ~bin_response sexp_of_response =
  let rpc =
    Async_rpc_kernel.Rpc.Rpc.create
      ~name:"rpc"
      ~version:1
      ~bin_query:[%bin_type_class: string]
      ~bin_response
  in
  let response1 = Ivar.create () in
  let response2 = Ivar.create () in
  let responses = Queue.of_list [ response1; response2 ] in
  let impl =
    Async_rpc_kernel.Rpc.Rpc.implement rpc (fun mock q ->
      print_s [%message "Implementation_called" q];
      let response = Queue.dequeue_exn responses in
      let%map.Eager_deferred r = Ivar.read response in
      Mock_peer.expect_message mock bin_response.reader [%sexp_of: response];
      Result.ok_exn r)
  in
  let impl =
    Async_rpc_kernel.Rpc.Implementations.create_exn
      ~implementations:[ impl ]
      ~on_unknown_rpc:`Raise
  in
  response1, response2, impl
;;

let rpc () =
  rpc'
    ~bin_response:[%bin_type_class: (string, string) Result.t]
    [%sexp_of: (string, string) Result.t]
;;

let write_query ?don't_read_yet ?(id = 123) t =
  Mock_peer.write_message
    ?don't_read_yet
    t
    [%bin_writer: string]
    (Query
       { tag = Protocol.Rpc_tag.of_string "rpc"
       ; version = 1
       ; id = Protocol.Query_id.of_int_exn id
       ; metadata = None
       ; data = [%string "example query (id = %{Int.to_string id})"]
       })
;;

let default_config : Mock_peer.Config.t =
  { when_reader_waits = `Carry_on_until_end_of_batch; when_waiting_done = `Read_more }
;;

let single_rpc_test () =
  let rpc_response, (_ : ((string, string) result, exn) result Ivar.t), implementations =
    rpc ()
  in
  let%map t = Mock_peer.create_and_connect' ~implementations default_config in
  t, rpc_response
;;

let two_rpc_test () =
  let rpc_response1, rpc_response2, implementations = rpc () in
  let%map t = Mock_peer.create_and_connect' ~implementations default_config in
  t, rpc_response1, rpc_response2
;;

let%expect_test "Single successful rpc implementation" =
  let%bind t, rpc_response = single_rpc_test () in
  write_query t;
  [%expect
    {|
    (Tracing_event
     ((event (Received Query)) (rpc (((name rpc) (version 1)))) (id 123)
      (payload_bytes 34)))
    (Implementation_called "example query (id = 123)") |}];
  Ivar.fill_exn rpc_response (Ok (Ok "example response"));
  let%bind () = Scheduler.yield_until_no_jobs_remain () in
  [%expect
    {|
    (Send (Response ((id 123) (data (Ok (Ok "example response"))))))
    (Tracing_event
     ((event (Sent (Response Single_succeeded))) (rpc (((name rpc) (version 1))))
      (id 123) (payload_bytes 1))) |}];
  return ()
;;

let%expect_test "Single successful rpc implementation, returning user-defined error" =
  let%bind t, rpc_response = single_rpc_test () in
  write_query t;
  [%expect
    {|
    (Tracing_event
     ((event (Received Query)) (rpc (((name rpc) (version 1)))) (id 123)
      (payload_bytes 34)))
    (Implementation_called "example query (id = 123)") |}];
  Ivar.fill_exn rpc_response (Ok (Error "user error"));
  let%bind () = Scheduler.yield_until_no_jobs_remain () in
  [%expect
    {|
    (Send (Response ((id 123) (data (Ok (Error "user error"))))))
    (Tracing_event
     ((event (Sent (Response Single_succeeded))) (rpc (((name rpc) (version 1))))
      (id 123) (payload_bytes 1))) |}];
  return ()
;;

let%expect_test "Single raising rpc implementation" =
  let%bind t, rpc_response = single_rpc_test () in
  write_query t;
  [%expect
    {|
    (Tracing_event
     ((event (Received Query)) (rpc (((name rpc) (version 1)))) (id 123)
      (payload_bytes 34)))
    (Implementation_called "example query (id = 123)") |}];
  Backtrace.elide := true;
  Ivar.fill_exn rpc_response (Error (Failure "injected exn"));
  let%bind () = Scheduler.yield_until_no_jobs_remain () in
  Backtrace.elide := false;
  [%expect
    {|
    (Send
     (Response
      ((id 123)
       (data
        (Error
         (Uncaught_exn
          ((location "server-side rpc computation")
           (exn
            (monitor.ml.Error (Failure "injected exn")
             ("<backtrace elided in test>"))))))))))
    (Tracing_event
     ((event (Sent (Response Single_or_streaming_error)))
      (rpc (((name rpc) (version 1)))) (id 123) (payload_bytes 1))) |}];
  return ()
;;

let%expect_test "Single rpc implementation raising rpc error" =
  let%bind t, rpc_response = single_rpc_test () in
  write_query t;
  [%expect
    {|
    (Tracing_event
     ((event (Received Query)) (rpc (((name rpc) (version 1)))) (id 123)
      (payload_bytes 34)))
    (Implementation_called "example query (id = 123)") |}];
  Backtrace.elide := true;
  Ivar.fill_exn
    rpc_response
    (Error
       (Async_rpc_kernel.Rpc_error.Rpc (Unknown (Atom "injected exn"), Info.createf "info")));
  let%bind () = Scheduler.yield_until_no_jobs_remain () in
  Backtrace.elide := false;
  [%expect
    {|
    (Send
     (Response
      ((id 123)
       (data
        (Error
         (Uncaught_exn
          ((location "server-side rpc computation")
           (exn
            (monitor.ml.Error (rpc_error.ml.Rpc (Unknown "injected exn") info)
             ("<backtrace elided in test>"))))))))))
    (Tracing_event
     ((event (Sent (Response Single_or_streaming_error)))
      (rpc (((name rpc) (version 1)))) (id 123) (payload_bytes 1))) |}];
  return ()
;;

let%expect_test "Single rpc implementation fails to send once" =
  let%bind t, rpc_response = single_rpc_test () in
  write_query t;
  [%expect
    {|
    (Tracing_event
     ((event (Received Query)) (rpc (((name rpc) (version 1)))) (id 123)
      (payload_bytes 34)))
    (Implementation_called "example query (id = 123)") |}];
  Mock_peer.enqueue_send_result t (Message_too_big { size = 100; max_message_size = 10 });
  Mock_peer.expect_message ~later:() t [%bin_reader: Nothing.t] [%sexp_of: Nothing.t];
  Ivar.fill_exn rpc_response (Ok (Ok "attempted response"));
  let%bind () = Scheduler.yield_until_no_jobs_remain () in
  [%expect
    {|
    (Send (Response ((id 123) (data (Ok (Ok "attempted response"))))))
    (Tracing_event
     ((event (Failed_to_send (Response Single_succeeded) Too_large))
      (rpc (((name rpc) (version 1)))) (id 123) (payload_bytes 100)))
    (Send
     (Response
      ((id 123)
       (data
        (Error
         (Write_error (Message_too_big ((size 100) (max_message_size 10))))))))) |}];
  return ()
;;

let%expect_test "Single rpc implementation fails to send twice" =
  let%bind t, rpc_response = single_rpc_test () in
  write_query t;
  [%expect
    {|
    (Tracing_event
     ((event (Received Query)) (rpc (((name rpc) (version 1)))) (id 123)
      (payload_bytes 34)))
    (Implementation_called "example query (id = 123)") |}];
  for i = 1 to 2 do
    Mock_peer.enqueue_send_result
      t
      (Message_too_big { size = i * 100; max_message_size = 10 })
  done;
  Mock_peer.expect_message ~later:() t [%bin_reader: Nothing.t] [%sexp_of: Nothing.t];
  Ivar.fill_exn rpc_response (Ok (Ok "attempted response"));
  let%bind () = Scheduler.yield_until_no_jobs_remain () in
  [%expect
    {|
    (Send (Response ((id 123) (data (Ok (Ok "attempted response"))))))
    (Tracing_event
     ((event (Failed_to_send (Response Single_succeeded) Too_large))
      (rpc (((name rpc) (version 1)))) (id 123) (payload_bytes 100)))
    (Send
     (Response
      ((id 123)
       (data
        (Error
         (Write_error (Message_too_big ((size 100) (max_message_size 10)))))))))
    (Close_started
     ("exn raised in RPC connection loop"
      (monitor.ml.Error
       ("Failed to send write error to client"
        ((error (Message_too_big ((size 100) (max_message_size 10))))
         (reason (Message_too_big ((size 200) (max_message_size 10))))))
       ("<backtrace elided in test>" "Caught by monitor RPC connection loop"))))
    Close_writer
    Close_reader
    Close_finished |}];
  return ()
;;

let%expect_test "Single rpc implementation fails to send user-defined error once" =
  let%bind t, rpc_response = single_rpc_test () in
  write_query t;
  [%expect
    {|
    (Tracing_event
     ((event (Received Query)) (rpc (((name rpc) (version 1)))) (id 123)
      (payload_bytes 34)))
    (Implementation_called "example query (id = 123)") |}];
  Mock_peer.enqueue_send_result t (Message_too_big { size = 100; max_message_size = 10 });
  Mock_peer.expect_message ~later:() t [%bin_reader: Nothing.t] [%sexp_of: Nothing.t];
  Ivar.fill_exn rpc_response (Ok (Error "user error"));
  let%bind () = Scheduler.yield_until_no_jobs_remain () in
  [%expect
    {|
    (Send (Response ((id 123) (data (Ok (Error "user error"))))))
    (Tracing_event
     ((event (Failed_to_send (Response Single_succeeded) Too_large))
      (rpc (((name rpc) (version 1)))) (id 123) (payload_bytes 100)))
    (Send
     (Response
      ((id 123)
       (data
        (Error
         (Write_error (Message_too_big ((size 100) (max_message_size 10))))))))) |}];
  return ()
;;

let%expect_test "Single rpc implementation fails to send twice" =
  let%bind t, rpc_response = single_rpc_test () in
  write_query t;
  [%expect
    {|
    (Tracing_event
     ((event (Received Query)) (rpc (((name rpc) (version 1)))) (id 123)
      (payload_bytes 34)))
    (Implementation_called "example query (id = 123)") |}];
  for i = 1 to 2 do
    Mock_peer.enqueue_send_result
      t
      (Message_too_big { size = i * 100; max_message_size = 10 })
  done;
  Mock_peer.expect_message ~later:() t [%bin_reader: Nothing.t] [%sexp_of: Nothing.t];
  Ivar.fill_exn rpc_response (Ok (Error "user error"));
  let%bind () = Scheduler.yield_until_no_jobs_remain () in
  [%expect
    {|
    (Send (Response ((id 123) (data (Ok (Error "user error"))))))
    (Tracing_event
     ((event (Failed_to_send (Response Single_succeeded) Too_large))
      (rpc (((name rpc) (version 1)))) (id 123) (payload_bytes 100)))
    (Send
     (Response
      ((id 123)
       (data
        (Error
         (Write_error (Message_too_big ((size 100) (max_message_size 10)))))))))
    (Close_started
     ("exn raised in RPC connection loop"
      (monitor.ml.Error
       ("Failed to send write error to client"
        ((error (Message_too_big ((size 100) (max_message_size 10))))
         (reason (Message_too_big ((size 200) (max_message_size 10))))))
       ("<backtrace elided in test>" "Caught by monitor RPC connection loop"))))
    Close_writer
    Close_reader
    Close_finished |}];
  return ()
;;

let%expect_test "two rpcs in one batch" =
  let%bind t, response1, response2 = two_rpc_test () in
  write_query ~don't_read_yet:() ~id:25 t;
  write_query ~id:75 t;
  [%expect
    {|
    (Tracing_event
     ((event (Received Query)) (rpc (((name rpc) (version 1)))) (id 25)
      (payload_bytes 33)))
    (Implementation_called "example query (id = 25)")
    (Tracing_event
     ((event (Received Query)) (rpc (((name rpc) (version 1)))) (id 75)
      (payload_bytes 33)))
    (Implementation_called "example query (id = 75)") |}];
  Ivar.fill_exn response2 (Ok (Error "example error 2"));
  Ivar.fill_exn response1 (Ok (Ok "example response 1"));
  let%bind () = Scheduler.yield_until_no_jobs_remain () in
  [%expect
    {|
    (Send (Response ((id 75) (data (Ok (Error "example error 2"))))))
    (Tracing_event
     ((event (Sent (Response Single_succeeded))) (rpc (((name rpc) (version 1))))
      (id 75) (payload_bytes 1)))
    (Send (Response ((id 25) (data (Ok (Ok "example response 1"))))))
    (Tracing_event
     ((event (Sent (Response Single_succeeded))) (rpc (((name rpc) (version 1))))
      (id 25) (payload_bytes 1))) |}];
  return ()
;;

let%expect_test "two immediately-returning rpcs in one batch" =
  let%bind t, response1, response2 = two_rpc_test () in
  Ivar.fill_exn response1 (Ok (Ok "example response 1"));
  Ivar.fill_exn response2 (Ok (Error "example error 2"));
  write_query ~don't_read_yet:() ~id:25 t;
  write_query ~id:75 t;
  [%expect
    {|
    (Tracing_event
     ((event (Received Query)) (rpc (((name rpc) (version 1)))) (id 25)
      (payload_bytes 33)))
    (Implementation_called "example query (id = 25)")
    (Send (Response ((id 25) (data (Ok (Ok "example response 1"))))))
    (Tracing_event
     ((event (Sent (Response Single_succeeded))) (rpc (((name rpc) (version 1))))
      (id 25) (payload_bytes 1)))
    (Tracing_event
     ((event (Received Query)) (rpc (((name rpc) (version 1)))) (id 75)
      (payload_bytes 33)))
    (Implementation_called "example query (id = 75)")
    (Send (Response ((id 75) (data (Ok (Error "example error 2"))))))
    (Tracing_event
     ((event (Sent (Response Single_succeeded))) (rpc (((name rpc) (version 1))))
      (id 75) (payload_bytes 1))) |}];
  let%bind () = Scheduler.yield_until_no_jobs_remain () in
  [%expect {| |}];
  return ()
;;
