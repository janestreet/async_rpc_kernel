open! Core
open! Async
open Import

let rpc' ~print_impl ~bin_response ~include_in_error_count sexp_of_response =
  let rpc =
    Async_rpc_kernel.Rpc.Rpc.create
      ~name:"rpc"
      ~version:1
      ~bin_query:[%bin_type_class: string]
      ~bin_response
      ~include_in_error_count
  in
  let response1 = Ivar.create () in
  let response2 = Ivar.create () in
  let responses = Queue.of_list [ response1; response2 ] in
  let impl =
    Async_rpc_kernel.Rpc.Rpc.implement rpc (fun mock q ->
      if print_impl then print_s [%message "Implementation_called" q];
      let response = Queue.dequeue_exn responses in
      let%map.Eager_deferred r = Ivar.read response in
      Mock_peer.expect_message mock bin_response.reader [%sexp_of: response];
      Result.ok_exn r)
  in
  let impl =
    Async_rpc_kernel.Rpc.Implementations.create_exn
      ~implementations:[ impl ]
      ~on_unknown_rpc:`Raise
      ~on_exception:Log_on_background_exn
  in
  response1, response2, impl
;;

let rpc () =
  rpc'
    ~print_impl:true
    ~bin_response:[%bin_type_class: (string, string) Result.t]
    ~include_in_error_count:Result
    [%sexp_of: (string, string) Result.t]
;;

let write_query ?metadata ?don't_read_yet ?(id = 123) t =
  Mock_peer.write_message
    ?don't_read_yet
    t
    [%bin_writer: string]
    (Query_v2
       { tag = Protocol.Rpc_tag.of_string "rpc"
       ; version = 1
       ; id = Protocol.Query_id.of_int_exn id
       ; metadata
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
     ((event (Received Query)) (rpc ((name rpc) (version 1))) (id 123)
      (payload_bytes 34)))
    (Implementation_called "example query (id = 123)")
    |}];
  Ivar.fill_exn rpc_response (Ok (Ok "example response"));
  let%bind () = Scheduler.yield_until_no_jobs_remain () in
  [%expect
    {|
    (Send
     (Response_v2
      ((id 123) (impl_menu_index (0)) (data (Ok (Ok "example response"))))))
    (Tracing_event
     ((event (Sent (Response Single_succeeded))) (rpc ((name rpc) (version 1)))
      (id 123) (payload_bytes 1)))
    |}];
  return ()
;;

let%expect_test "Single successful rpc implementation, returning user-defined error" =
  let%bind t, rpc_response = single_rpc_test () in
  write_query t;
  [%expect
    {|
    (Tracing_event
     ((event (Received Query)) (rpc ((name rpc) (version 1))) (id 123)
      (payload_bytes 34)))
    (Implementation_called "example query (id = 123)")
    |}];
  Ivar.fill_exn rpc_response (Ok (Error "user error"));
  let%bind () = Scheduler.yield_until_no_jobs_remain () in
  [%expect
    {|
    (Send
     (Response_v2
      ((id 123) (impl_menu_index (0)) (data (Ok (Error "user error"))))))
    (Tracing_event
     ((event (Sent (Response Single_or_streaming_user_defined_error)))
      (rpc ((name rpc) (version 1))) (id 123) (payload_bytes 1)))
    |}];
  return ()
;;

let%expect_test "Single raising rpc implementation" =
  let%bind t, rpc_response = single_rpc_test () in
  write_query t;
  [%expect
    {|
    (Tracing_event
     ((event (Received Query)) (rpc ((name rpc) (version 1))) (id 123)
      (payload_bytes 34)))
    (Implementation_called "example query (id = 123)")
    |}];
  Dynamic.set_root Backtrace.elide true;
  Ivar.fill_exn rpc_response (Error (Failure "injected exn"));
  let%bind () = Scheduler.yield_until_no_jobs_remain () in
  Dynamic.set_root Backtrace.elide false;
  [%expect
    {|
    (Send
     (Response_v2
      ((id 123) (impl_menu_index (0))
       (data
        (Error
         (Uncaught_exn
          ((location "server-side rpc computation")
           (exn
            (monitor.ml.Error (Failure "injected exn")
             ("<backtrace elided in test>"))))))))))
    (Tracing_event
     ((event (Sent (Response Single_or_streaming_rpc_error_or_exn)))
      (rpc ((name rpc) (version 1))) (id 123) (payload_bytes 1)))
    |}];
  return ()
;;

let%expect_test "Single rpc implementation raising rpc error" =
  let%bind t, rpc_response = single_rpc_test () in
  write_query t;
  [%expect
    {|
    (Tracing_event
     ((event (Received Query)) (rpc ((name rpc) (version 1))) (id 123)
      (payload_bytes 34)))
    (Implementation_called "example query (id = 123)")
    |}];
  Dynamic.set_root Backtrace.elide true;
  Ivar.fill_exn
    rpc_response
    (Error
       (Async_rpc_kernel.Rpc_error.Rpc (Unknown (Atom "injected exn"), Info.createf "info")));
  let%bind () = Scheduler.yield_until_no_jobs_remain () in
  Dynamic.set_root Backtrace.elide false;
  [%expect
    {|
    (Send
     (Response_v2
      ((id 123) (impl_menu_index (0))
       (data
        (Error
         (Uncaught_exn
          ((location "server-side rpc computation")
           (exn
            (monitor.ml.Error (rpc_error.ml.Rpc (Unknown "injected exn") info)
             ("<backtrace elided in test>"))))))))))
    (Tracing_event
     ((event (Sent (Response Single_or_streaming_rpc_error_or_exn)))
      (rpc ((name rpc) (version 1))) (id 123) (payload_bytes 1)))
    |}];
  return ()
;;

let%expect_test "Single rpc implementation fails to send once" =
  let%bind t, rpc_response = single_rpc_test () in
  write_query t;
  [%expect
    {|
    (Tracing_event
     ((event (Received Query)) (rpc ((name rpc) (version 1))) (id 123)
      (payload_bytes 34)))
    (Implementation_called "example query (id = 123)")
    |}];
  Mock_peer.enqueue_send_result t (Message_too_big { size = 100; max_message_size = 10 });
  Mock_peer.expect_message ~later:() t [%bin_reader: Nothing.t] [%sexp_of: Nothing.t];
  Ivar.fill_exn rpc_response (Ok (Ok "attempted response"));
  let%bind () = Scheduler.yield_until_no_jobs_remain () in
  [%expect
    {|
    (Send
     (Response_v2
      ((id 123) (impl_menu_index (0)) (data (Ok (Ok "attempted response"))))))
    (Tracing_event
     ((event (Failed_to_send (Response Single_succeeded) Too_large))
      (rpc ((name rpc) (version 1))) (id 123) (payload_bytes 100)))
    (Send
     (Response_v2
      ((id 123) (impl_menu_index (0))
       (data
        (Error
         (Write_error (Message_too_big ((size 100) (max_message_size 10)))))))))
    |}];
  return ()
;;

let%expect_test "Single rpc implementation fails to send twice" =
  let%bind t, rpc_response = single_rpc_test () in
  write_query t;
  [%expect
    {|
    (Tracing_event
     ((event (Received Query)) (rpc ((name rpc) (version 1))) (id 123)
      (payload_bytes 34)))
    (Implementation_called "example query (id = 123)")
    |}];
  Dynamic.set_root Backtrace.elide true;
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
    (Send
     (Response_v2
      ((id 123) (impl_menu_index (0)) (data (Ok (Ok "attempted response"))))))
    (Tracing_event
     ((event (Failed_to_send (Response Single_succeeded) Too_large))
      (rpc ((name rpc) (version 1))) (id 123) (payload_bytes 100)))
    (Send
     (Response_v2
      ((id 123) (impl_menu_index (0))
       (data
        (Error
         (Write_error (Message_too_big ((size 100) (max_message_size 10)))))))))
    (Send
     (message
      ("00000000  05 05 21 65 78 6e 20 72  61 69 73 65 64 20 69 6e  |..!exn raised in|"
       "00000010  20 52 50 43 20 63 6f 6e  6e 65 63 74 69 6f 6e 20  | RPC connection |"
       "00000020  6c 6f 6f 70 02 01 03 00  10 6d 6f 6e 69 74 6f 72  |loop.....monitor|"
       "00000030  2e 6d 6c 2e 45 72 72 6f  72 01 02 00 24 46 61 69  |.ml.Error...$Fai|"
       "00000040  6c 65 64 20 74 6f 20 73  65 6e 64 20 77 72 69 74  |led to send writ|"
       "00000050  65 20 65 72 72 6f 72 20  74 6f 20 63 6c 69 65 6e  |e error to clien|"
       "00000060  74 01 02 01 02 00 05 65  72 72 6f 72 01 02 00 0f  |t......error....|"
       "00000070  4d 65 73 73 61 67 65 5f  74 6f 6f 5f 62 69 67 01  |Message_too_big.|"
       "00000080  02 01 02 00 04 73 69 7a  65 00 03 31 30 30 01 02  |.....size..100..|"
       "00000090  00 10 6d 61 78 5f 6d 65  73 73 61 67 65 5f 73 69  |..max_message_si|"
       "000000a0  7a 65 00 02 31 30 01 02  00 06 72 65 61 73 6f 6e  |ze..10....reason|"
       "000000b0  01 02 00 0f 4d 65 73 73  61 67 65 5f 74 6f 6f 5f  |....Message_too_|"
       "000000c0  62 69 67 01 02 01 02 00  04 73 69 7a 65 00 03 32  |big......size..2|"
       "000000d0  30 30 01 02 00 10 6d 61  78 5f 6d 65 73 73 61 67  |00....max_messag|"
       "000000e0  65 5f 73 69 7a 65 00 02  31 30 01 02 00 1a 3c 62  |e_size..10....<b|"
       "000000f0  61 63 6b 74 72 61 63 65  20 65 6c 69 64 65 64 20  |acktrace elided |"
       "00000100  69 6e 20 74 65 73 74 3e  00 25 43 61 75 67 68 74  |in test>.%Caught|"
       "00000110  20 62 79 20 6d 6f 6e 69  74 6f 72 20 52 50 43 20  | by monitor RPC |"
       "00000120  63 6f 6e 6e 65 63 74 69  6f 6e 20 6c 6f 6f 70     |connection loop|")))
    (Close_started
     (("exn raised in RPC connection loop"
       (monitor.ml.Error
        ("Failed to send write error to client"
         ((error (Message_too_big ((size 100) (max_message_size 10))))
          (reason (Message_too_big ((size 200) (max_message_size 10))))))
        ("<backtrace elided in test>" "Caught by monitor RPC connection loop")))
      (connection_description <created-directly>)))
    Close_writer
    Close_reader
    Close_finished
    |}];
  return ()
;;

let%expect_test "connection closed for single rpc response" =
  let%bind t, rpc_response = single_rpc_test () in
  write_query t;
  [%expect
    {|
    (Tracing_event
     ((event (Received Query)) (rpc ((name rpc) (version 1))) (id 123)
      (payload_bytes 34)))
    (Implementation_called "example query (id = 123)")
    |}];
  for _ = 1 to 2 do
    Mock_peer.enqueue_send_result t Closed
  done;
  Mock_peer.expect_message ~later:() t [%bin_reader: Nothing.t] [%sexp_of: Nothing.t];
  Ivar.fill_exn rpc_response (Ok (Ok "attempted response"));
  let%bind () = Scheduler.yield_until_no_jobs_remain () in
  [%expect
    {|
    (Send
     (Response_v2
      ((id 123) (impl_menu_index (0)) (data (Ok (Ok "attempted response"))))))
    (Tracing_event
     ((event (Failed_to_send (Response Single_succeeded) Closed))
      (rpc ((name rpc) (version 1))) (id 123) (payload_bytes 0)))
    |}];
  return ()
;;

let%expect_test "connection fully closes before single rpc response" =
  let%bind t, rpc_response = single_rpc_test () in
  write_query t;
  [%expect
    {|
    (Tracing_event
     ((event (Received Query)) (rpc ((name rpc) (version 1))) (id 123)
      (payload_bytes 34)))
    (Implementation_called "example query (id = 123)")
    |}];
  Mock_peer.close_reader t;
  let%bind () = Scheduler.yield_until_no_jobs_remain () in
  [%expect
    {|
    (Close_started
     ("EOF or connection closed" (connection_description <created-directly>)))
    Close_writer
    Close_reader
    Close_finished
    |}];
  for _ = 1 to 2 do
    Mock_peer.enqueue_send_result t Closed
  done;
  Mock_peer.expect_message ~later:() t [%bin_reader: Nothing.t] [%sexp_of: Nothing.t];
  Ivar.fill_exn rpc_response (Ok (Ok "attempted response"));
  let%bind () = Scheduler.yield_until_no_jobs_remain () in
  [%expect {| |}];
  return ()
;;

let%expect_test "Single rpc implementation fails to send user-defined error once" =
  let%bind t, rpc_response = single_rpc_test () in
  write_query t;
  [%expect
    {|
    (Tracing_event
     ((event (Received Query)) (rpc ((name rpc) (version 1))) (id 123)
      (payload_bytes 34)))
    (Implementation_called "example query (id = 123)")
    |}];
  Mock_peer.enqueue_send_result t (Message_too_big { size = 100; max_message_size = 10 });
  Mock_peer.expect_message ~later:() t [%bin_reader: Nothing.t] [%sexp_of: Nothing.t];
  Ivar.fill_exn rpc_response (Ok (Error "user error"));
  let%bind () = Scheduler.yield_until_no_jobs_remain () in
  [%expect
    {|
    (Send
     (Response_v2
      ((id 123) (impl_menu_index (0)) (data (Ok (Error "user error"))))))
    (Tracing_event
     ((event
       (Failed_to_send (Response Single_or_streaming_user_defined_error)
        Too_large))
      (rpc ((name rpc) (version 1))) (id 123) (payload_bytes 100)))
    (Send
     (Response_v2
      ((id 123) (impl_menu_index (0))
       (data
        (Error
         (Write_error (Message_too_big ((size 100) (max_message_size 10)))))))))
    |}];
  return ()
;;

let%expect_test "Single rpc implementation fails to send twice" =
  let%bind t, rpc_response = single_rpc_test () in
  write_query t;
  [%expect
    {|
    (Tracing_event
     ((event (Received Query)) (rpc ((name rpc) (version 1))) (id 123)
      (payload_bytes 34)))
    (Implementation_called "example query (id = 123)")
    |}];
  Dynamic.set_root Backtrace.elide true;
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
    (Send
     (Response_v2
      ((id 123) (impl_menu_index (0)) (data (Ok (Error "user error"))))))
    (Tracing_event
     ((event
       (Failed_to_send (Response Single_or_streaming_user_defined_error)
        Too_large))
      (rpc ((name rpc) (version 1))) (id 123) (payload_bytes 100)))
    (Send
     (Response_v2
      ((id 123) (impl_menu_index (0))
       (data
        (Error
         (Write_error (Message_too_big ((size 100) (max_message_size 10)))))))))
    (Send
     (message
      ("00000000  05 05 21 65 78 6e 20 72  61 69 73 65 64 20 69 6e  |..!exn raised in|"
       "00000010  20 52 50 43 20 63 6f 6e  6e 65 63 74 69 6f 6e 20  | RPC connection |"
       "00000020  6c 6f 6f 70 02 01 03 00  10 6d 6f 6e 69 74 6f 72  |loop.....monitor|"
       "00000030  2e 6d 6c 2e 45 72 72 6f  72 01 02 00 24 46 61 69  |.ml.Error...$Fai|"
       "00000040  6c 65 64 20 74 6f 20 73  65 6e 64 20 77 72 69 74  |led to send writ|"
       "00000050  65 20 65 72 72 6f 72 20  74 6f 20 63 6c 69 65 6e  |e error to clien|"
       "00000060  74 01 02 01 02 00 05 65  72 72 6f 72 01 02 00 0f  |t......error....|"
       "00000070  4d 65 73 73 61 67 65 5f  74 6f 6f 5f 62 69 67 01  |Message_too_big.|"
       "00000080  02 01 02 00 04 73 69 7a  65 00 03 31 30 30 01 02  |.....size..100..|"
       "00000090  00 10 6d 61 78 5f 6d 65  73 73 61 67 65 5f 73 69  |..max_message_si|"
       "000000a0  7a 65 00 02 31 30 01 02  00 06 72 65 61 73 6f 6e  |ze..10....reason|"
       "000000b0  01 02 00 0f 4d 65 73 73  61 67 65 5f 74 6f 6f 5f  |....Message_too_|"
       "000000c0  62 69 67 01 02 01 02 00  04 73 69 7a 65 00 03 32  |big......size..2|"
       "000000d0  30 30 01 02 00 10 6d 61  78 5f 6d 65 73 73 61 67  |00....max_messag|"
       "000000e0  65 5f 73 69 7a 65 00 02  31 30 01 02 00 1a 3c 62  |e_size..10....<b|"
       "000000f0  61 63 6b 74 72 61 63 65  20 65 6c 69 64 65 64 20  |acktrace elided |"
       "00000100  69 6e 20 74 65 73 74 3e  00 25 43 61 75 67 68 74  |in test>.%Caught|"
       "00000110  20 62 79 20 6d 6f 6e 69  74 6f 72 20 52 50 43 20  | by monitor RPC |"
       "00000120  63 6f 6e 6e 65 63 74 69  6f 6e 20 6c 6f 6f 70     |connection loop|")))
    (Close_started
     (("exn raised in RPC connection loop"
       (monitor.ml.Error
        ("Failed to send write error to client"
         ((error (Message_too_big ((size 100) (max_message_size 10))))
          (reason (Message_too_big ((size 200) (max_message_size 10))))))
        ("<backtrace elided in test>" "Caught by monitor RPC connection loop")))
      (connection_description <created-directly>)))
    Close_writer
    Close_reader
    Close_finished
    |}];
  return ()
;;

let%expect_test "two rpcs in one batch" =
  let%bind t, response1, response2 = two_rpc_test () in
  write_query ~don't_read_yet:() ~id:25 t;
  write_query ~id:75 t;
  [%expect
    {|
    (Tracing_event
     ((event (Received Query)) (rpc ((name rpc) (version 1))) (id 25)
      (payload_bytes 33)))
    (Implementation_called "example query (id = 25)")
    (Tracing_event
     ((event (Received Query)) (rpc ((name rpc) (version 1))) (id 75)
      (payload_bytes 33)))
    (Implementation_called "example query (id = 75)")
    |}];
  Ivar.fill_exn response2 (Ok (Error "example error 2"));
  Ivar.fill_exn response1 (Ok (Ok "example response 1"));
  let%bind () = Scheduler.yield_until_no_jobs_remain () in
  [%expect
    {|
    (Send
     (Response_v2
      ((id 75) (impl_menu_index (0)) (data (Ok (Error "example error 2"))))))
    (Tracing_event
     ((event (Sent (Response Single_or_streaming_user_defined_error)))
      (rpc ((name rpc) (version 1))) (id 75) (payload_bytes 1)))
    (Send
     (Response_v2
      ((id 25) (impl_menu_index (0)) (data (Ok (Ok "example response 1"))))))
    (Tracing_event
     ((event (Sent (Response Single_succeeded))) (rpc ((name rpc) (version 1)))
      (id 25) (payload_bytes 1)))
    |}];
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
     ((event (Received Query)) (rpc ((name rpc) (version 1))) (id 25)
      (payload_bytes 33)))
    (Implementation_called "example query (id = 25)")
    (Send
     (Response_v2
      ((id 25) (impl_menu_index (0)) (data (Ok (Ok "example response 1"))))))
    (Tracing_event
     ((event (Sent (Response Single_succeeded))) (rpc ((name rpc) (version 1)))
      (id 25) (payload_bytes 1)))
    (Tracing_event
     ((event (Received Query)) (rpc ((name rpc) (version 1))) (id 75)
      (payload_bytes 33)))
    (Implementation_called "example query (id = 75)")
    (Send
     (Response_v2
      ((id 75) (impl_menu_index (0)) (data (Ok (Error "example error 2"))))))
    (Tracing_event
     ((event (Sent (Response Single_or_streaming_user_defined_error)))
      (rpc ((name rpc) (version 1))) (id 75) (payload_bytes 1)))
    |}];
  let%bind () = Scheduler.yield_until_no_jobs_remain () in
  [%expect {| |}];
  return ()
;;

let%expect_test "recognising errors in responses" =
  let test bin_response sexp_of_response include_in_error_count response =
    Dynamic.set_root Backtrace.elide true;
    let response1, (_response2 : (_, exn) Result.t Ivar.t), implementations =
      rpc' ~print_impl:false ~bin_response ~include_in_error_count sexp_of_response
    in
    let%bind t = Mock_peer.create_and_connect' ~implementations default_config in
    Ivar.fill_exn
      response1
      (match response with
       | `Return r -> Ok r
       | `Raise -> Error (Failure "exn"));
    let print_part = printf "%-22s" in
    Mock_peer.set_on_emit t (function
      | Tracing_event { rpc = _; id = _; payload_bytes = _; event = Sent (Response kind) }
        -> print_part (Sexp.to_string [%sexp (kind : Tracing_event.Sent_response_kind.t)])
      | Tracing_event { rpc = _; id = _; payload_bytes = _; event = Received Query } ->
        (* For the test, queries are not important to print out. *)
        ()
      | Write ev ->
        let bs = Mock_peer.Write_event.bigstring_written ev in
        let data =
          Bin_prot.Reader.of_bigstring
            (Protocol.Message.bin_reader_t
               (Binio_printer_helper.With_length.bin_reader_t bin_response.reader))
            bs
        in
        (match data with
         | Response_v1 { id = _; data = Ok x }
         | Response_v2 { id = _; impl_menu_index = _; data = Ok x } ->
           print_part (Sexp.to_string [%message "sent" ~_:(x : response)])
         | Response_v1 { id = _; data = Error (Uncaught_exn _) }
         | Response_v2 { id = _; impl_menu_index = _; data = Error (Uncaught_exn _) } ->
           print_part "sent Uncaught_exn"
         | data ->
           print_endline "";
           print_s [%message "unexpected write" ~_:(data : response Protocol.Message.t)])
      | e ->
        print_endline "";
        print_s [%message "unexpected event" ~_:(e : Mock_peer.Event.t)]);
    write_query t;
    let%map () = Scheduler.yield_until_no_jobs_remain () in
    Dynamic.set_root Backtrace.elide false
  in
  let f e = test [%bin_type_class: string] [%sexp_of: string] e in
  let%bind () = f Only_on_exn (`Return "x") in
  [%expect {| (sent x)              Single_succeeded |}];
  let%bind () = f Only_on_exn `Raise in
  [%expect {| sent Uncaught_exn     Single_or_streaming_rpc_error_or_exn |}];
  let%bind () = f (Custom { is_error = String.is_empty }) `Raise in
  [%expect {| sent Uncaught_exn     Single_or_streaming_rpc_error_or_exn |}];
  let%bind () = f (Custom { is_error = String.is_empty }) (`Return "x") in
  [%expect {| (sent x)              Single_succeeded |}];
  let%bind () = f (Custom { is_error = String.is_empty }) (`Return "") in
  [%expect {| (sent"")              Single_or_streaming_user_defined_error |}];
  let%bind () = f (Custom { is_error = failwith }) (`Return "x") in
  [%expect {| (sent x)              Single_or_streaming_rpc_error_or_exn |}];
  let error = error_s [%sexp ()] in
  let f = test [%bin_type_class: bool Or_error.t] [%sexp_of: bool Or_error.t] in
  let%bind () = f Only_on_exn (`Return error) in
  [%expect {| (sent(Error()))       Single_succeeded |}];
  let%bind () = f Or_error (`Return (Ok true)) in
  [%expect {| (sent(Ok true))       Single_succeeded |}];
  let%bind () = f Or_error (`Return error) in
  [%expect {| (sent(Error()))       Single_or_streaming_user_defined_error |}];
  let%bind () = f Or_error `Raise in
  [%expect {| sent Uncaught_exn     Single_or_streaming_rpc_error_or_exn |}];
  let%bind () = f Result (`Return (Ok true)) in
  [%expect {| (sent(Ok true))       Single_succeeded |}];
  let%bind () = f Result (`Return error) in
  [%expect {| (sent(Error()))       Single_or_streaming_user_defined_error |}];
  let%bind () = f Result `Raise in
  [%expect {| sent Uncaught_exn     Single_or_streaming_rpc_error_or_exn |}];
  let%bind () = f (Generic_or_error Only_on_exn) (`Return (Ok true)) in
  [%expect {| (sent(Ok true))       Single_succeeded |}];
  let%bind () = f (Generic_or_error Only_on_exn) (`Return error) in
  [%expect {| (sent(Error()))       Single_or_streaming_user_defined_error |}];
  let%bind () = f (Generic_or_error Only_on_exn) `Raise in
  [%expect {| sent Uncaught_exn     Single_or_streaming_rpc_error_or_exn |}];
  let%bind () = f (Generic_or_error (Custom { is_error = Fn.id })) (`Return (Ok false)) in
  [%expect {| (sent(Ok false))      Single_succeeded |}];
  let%bind () = f (Generic_or_error (Custom { is_error = Fn.id })) (`Return (Ok true)) in
  [%expect {| (sent(Ok true))       Single_or_streaming_user_defined_error |}];
  let%bind () = f (Generic_or_error (Custom { is_error = Fn.id })) (`Return error) in
  [%expect {| (sent(Error()))       Single_or_streaming_user_defined_error |}];
  let%bind () = f (Generic_result Only_on_exn) (`Return (Ok true)) in
  [%expect {| (sent(Ok true))       Single_succeeded |}];
  let%bind () = f (Generic_result Only_on_exn) (`Return error) in
  [%expect {| (sent(Error()))       Single_or_streaming_user_defined_error |}];
  let%bind () = f (Generic_result Only_on_exn) `Raise in
  [%expect {| sent Uncaught_exn     Single_or_streaming_rpc_error_or_exn |}];
  let%bind () = f (Generic_result (Custom { is_error = Fn.id })) (`Return (Ok false)) in
  [%expect {| (sent(Ok false))      Single_succeeded |}];
  let%bind () = f (Generic_result (Custom { is_error = Fn.id })) (`Return (Ok true)) in
  [%expect {| (sent(Ok true))       Single_or_streaming_user_defined_error |}];
  let%bind () = f (Generic_result (Custom { is_error = Fn.id })) (`Return error) in
  [%expect {| (sent(Error()))       Single_or_streaming_user_defined_error |}];
  let f =
    test
      [%bin_type_class: bool Or_error.t Or_error.t]
      [%sexp_of: bool Or_error.t Or_error.t]
  in
  let%bind () = f Result (`Return (Ok error)) in
  [%expect {| (sent(Ok(Error())))   Single_succeeded |}];
  let%bind () = f Or_error_or_error `Raise in
  [%expect {| sent Uncaught_exn     Single_or_streaming_rpc_error_or_exn |}];
  let%bind () = f Or_error_or_error (`Return error) in
  [%expect {| (sent(Error()))       Single_or_streaming_user_defined_error |}];
  let%bind () = f Or_error_or_error (`Return (Ok error)) in
  [%expect {| (sent(Ok(Error())))   Single_or_streaming_user_defined_error |}];
  let%bind () = f Or_error_or_error (`Return (Ok (Ok true))) in
  [%expect {| (sent(Ok(Ok true)))   Single_succeeded |}];
  let%bind () = f Or_error_or_error (`Return (Ok (Ok true))) in
  [%expect {| (sent(Ok(Ok true)))   Single_succeeded |}];
  let%bind () = f Or_error_result `Raise in
  [%expect {| sent Uncaught_exn     Single_or_streaming_rpc_error_or_exn |}];
  let%bind () = f Or_error_result (`Return error) in
  [%expect {| (sent(Error()))       Single_or_streaming_user_defined_error |}];
  let%bind () = f Or_error_result (`Return (Ok error)) in
  [%expect {| (sent(Ok(Error())))   Single_or_streaming_user_defined_error |}];
  let%bind () = f Or_error_result (`Return (Ok (Ok true))) in
  [%expect {| (sent(Ok(Ok true)))   Single_succeeded |}];
  let%bind () = f Result_or_error `Raise in
  [%expect {| sent Uncaught_exn     Single_or_streaming_rpc_error_or_exn |}];
  let%bind () = f Result_or_error (`Return error) in
  [%expect {| (sent(Error()))       Single_or_streaming_user_defined_error |}];
  let%bind () = f Result_or_error (`Return (Ok error)) in
  [%expect {| (sent(Ok(Error())))   Single_or_streaming_user_defined_error |}];
  let%bind () = f Result_or_error (`Return (Ok (Ok true))) in
  [%expect {| (sent(Ok(Ok true)))   Single_succeeded |}];
  let%bind () = f Result_result `Raise in
  [%expect {| sent Uncaught_exn     Single_or_streaming_rpc_error_or_exn |}];
  let%bind () = f Result_result (`Return error) in
  [%expect {| (sent(Error()))       Single_or_streaming_user_defined_error |}];
  let%bind () = f Result_result (`Return (Ok error)) in
  [%expect {| (sent(Ok(Error())))   Single_or_streaming_user_defined_error |}];
  let%bind () = f Result_result (`Return (Ok (Ok true))) in
  [%expect {| (sent(Ok(Ok true)))   Single_succeeded |}];
  let f2 x = f (Generic_or_error (Generic_or_error (Custom { is_error = Fn.id }))) x in
  let%bind () = f2 `Raise in
  [%expect {| sent Uncaught_exn     Single_or_streaming_rpc_error_or_exn |}];
  let%bind () = f2 (`Return error) in
  [%expect {| (sent(Error()))       Single_or_streaming_user_defined_error |}];
  let%bind () = f2 (`Return (Ok error)) in
  [%expect {| (sent(Ok(Error())))   Single_or_streaming_user_defined_error |}];
  let%bind () = f2 (`Return (Ok (Ok true))) in
  [%expect {| (sent(Ok(Ok true)))   Single_or_streaming_user_defined_error |}];
  let%bind () = f2 (`Return (Ok (Ok false))) in
  [%expect {| (sent(Ok(Ok false)))  Single_succeeded |}];
  let f2 x = f (Generic_result (Generic_result (Custom { is_error = Fn.id }))) x in
  let%bind () = f2 `Raise in
  [%expect {| sent Uncaught_exn     Single_or_streaming_rpc_error_or_exn |}];
  let%bind () = f2 (`Return error) in
  [%expect {| (sent(Error()))       Single_or_streaming_user_defined_error |}];
  let%bind () = f2 (`Return (Ok error)) in
  [%expect {| (sent(Ok(Error())))   Single_or_streaming_user_defined_error |}];
  let%bind () = f2 (`Return (Ok (Ok true))) in
  [%expect {| (sent(Ok(Ok true)))   Single_or_streaming_user_defined_error |}];
  let%bind () = f2 (`Return (Ok (Ok false))) in
  [%expect {| (sent(Ok(Ok false)))  Single_succeeded |}];
  let f =
    test
      [%bin_type_class: int * unit Or_error.t]
      [%sexp_of: int * unit Or_error.t]
      (Embedded (snd, Or_error))
  in
  let%bind () = f `Raise in
  [%expect {| sent Uncaught_exn     Single_or_streaming_rpc_error_or_exn |}];
  let%bind () = f (`Return (1, Ok ())) in
  [%expect {| (sent(1(Ok())))       Single_succeeded |}];
  let%bind () = f (`Return (1, error)) in
  [%expect {| (sent(1(Error())))    Single_or_streaming_user_defined_error |}];
  return ()
;;

let%expect_test "connection closes before response received" =
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
    (Send (Query_v2 ((tag rpc) (version 1) (id 1) (metadata ()) (data query))))
    (Tracing_event
     ((event (Sent Query)) (rpc ((name rpc) (version 1))) (id 1)
      (payload_bytes 1)))
    |}];
  Mock_peer.close_reader t;
  let%bind () = Scheduler.yield_until_no_jobs_remain () in
  [%expect
    {|
    (Close_started
     ("EOF or connection closed" (connection_description <created-directly>)))
    Close_writer
    Close_reader
    Close_finished
    |}];
  let%bind result in
  print_s ([%sexp_of: unit Protocol.Rpc_result.t] result);
  [%expect {| (Error Connection_closed) |}];
  return ()
;;

let%expect_test "expert unknown rpc handler" =
  let implementations =
    Rpc.Implementations.Expert.create_exn
      ~implementations:[]
      ~on_unknown_rpc:
        (`Expert
          (fun (_ : Mock_peer.t)
            ~rpc_tag
            ~version
            ~metadata
            responder
            bs
            ~pos
            ~len:(_ : int) ->
            print_s
              [%message
                "Unknown rpc"
                  ~rpc_tag
                  (version : int)
                  (metadata : Async_rpc_kernel.Rpc_metadata.V1.t option)
                  ~data:([%bin_read: string] bs ~pos_ref:(ref pos))];
            Rpc.Rpc.Expert.Responder.write_error
              responder
              (Error.create_s [%message "example error"]);
            return ()))
      ~on_exception:Log_on_background_exn
  in
  let%bind t = Mock_peer.create_and_connect' ~implementations default_config in
  Mock_peer.expect_message t [%bin_reader: Nothing.t] [%sexp_of: Nothing.t];
  write_query t;
  [%expect
    {|
    (Tracing_event
     ((event (Received Query)) (rpc ((name rpc) (version 1))) (id 123)
      (payload_bytes 34)))
    ("Unknown rpc" (rpc_tag rpc) (version 1) (metadata ())
     (data "example query (id = 123)"))
    (Send
     (Response_v2
      ((id 123) (impl_menu_index ())
       (data
        (Error
         (Uncaught_exn
          ((location "server-side raw rpc computation") (exn "example error"))))))))
    (Tracing_event
     ((event (Sent (Response Expert_single_succeeded_or_failed)))
      (rpc ((name rpc) (version 1))) (id 123) (payload_bytes 0)))
    |}];
  Mock_peer.expect_message t [%bin_reader: Nothing.t] [%sexp_of: Nothing.t];
  write_query ~metadata:(Async_rpc_kernel.Rpc_metadata.V1.of_string "example metadata") t;
  [%expect
    {|
    (Tracing_event
     ((event (Received Query)) (rpc ((name rpc) (version 1))) (id 123)
      (payload_bytes 51)))
    ("Unknown rpc" (rpc_tag rpc) (version 1) (metadata ("example metadata"))
     (data "example query (id = 123)"))
    (Send
     (Response_v2
      ((id 123) (impl_menu_index ())
       (data
        (Error
         (Uncaught_exn
          ((location "server-side raw rpc computation") (exn "example error"))))))))
    (Tracing_event
     ((event (Sent (Response Expert_single_succeeded_or_failed)))
      (rpc ((name rpc) (version 1))) (id 123) (payload_bytes 0)))
    |}];
  return ()
;;
