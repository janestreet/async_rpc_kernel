open! Core
open! Async
open Import

let%expect_test "connect and close" =
  let t =
    Mock_peer.create
      { when_reader_waits = `Carry_on_until_end_of_batch; when_waiting_done = `Read_more }
  in
  Mock_peer.expect
    t
    [%bin_reader: Test_helpers.Header.t]
    [%sexp_of: Test_helpers.Header.t];
  Mock_peer.expect
    t
    [%bin_reader: Protocol.Message.nat0_t]
    [%sexp_of: _ Protocol.Message.t];
  let%bind conn = Mock_peer.connect t >>| Result.ok_exn in
  [%expect
    {|
    (Send (4411474 1 2 3 4 5 6 7 8 9 10 11))
    (Send
     (Metadata_v2
      ((identification ()) (menu (((descriptions ()) (digests ())))))))
    |}];
  let%bind () = Scheduler.yield_until_no_jobs_remain () in
  [%expect {| (Send Heartbeat) |}];
  let%bind () = Async_rpc_kernel.Rpc.Connection.close conn in
  [%expect
    {|
    (Close_started
     (("Connection closed by local side:" Rpc.Connection.close)
      (connection_description <created-directly>)))
    Close_writer
    Close_reader
    Close_finished
    |}];
  return ()
;;

let%expect_test "close immediately after handshake (with close message sent)" =
  let t =
    Mock_peer.create
      { when_reader_waits = `Carry_on_until_end_of_batch; when_waiting_done = `Read_more }
  in
  Mock_peer.write_handshake ~don't_read_yet:() t `v4;
  Mock_peer.write_message
    t
    Protocol.Message.bin_writer_nat0_t
    (Protocol.Message.Close_reason (Info.create_s [%message "immediate close"]));
  let%map () =
    match%bind Mock_peer.connect t with
    | Ok conn -> Rpc.Connection.close_finished conn
    | Error error -> error |> [%sexp_of: exn] |> print_s |> return
  in
  [%expect
    {|
    (Send
     (message
      ("00000000  0c fd 52 50 43 00 01 02  03 04 05 06 07 08 09 0a  |..RPC...........|"
       "00000010  0b                                                |.|")))
    (Send
     (message
      ("00000000  04 00 01 00                                       |....|")))
    Close_writer
    Close_reader
    (handshake_error.ml.Handshake_error
     ((Transport_closed_with_reason_from_remote_during_step
       (step Connection_metadata) (close_reason "immediate close"))
      <created-directly>))
    |}]
;;

let%expect_test "close with grace period" =
  let time_source = Time_source.create ~now:Time_ns.epoch () in
  let rpc =
    Rpc.Rpc.create
      ~name:"unit"
      ~version:1
      ~bin_query:[%bin_type_class: unit]
      ~bin_response:[%bin_type_class: unit]
      ~include_in_error_count:Only_on_exn
  in
  let implementations =
    let implementations =
      Rpc.Rpc.implement rpc (fun _ () ->
        Time_source.after time_source Time_ns.Span.second)
      |> List.singleton
    in
    Rpc.Implementations.create_exn
      ~implementations
      ~on_unknown_rpc:`Raise
      ~on_exception:Close_connection
  in
  let t =
    Mock_peer.create
      ~time_source:(time_source |> Time_source.read_only |> Time_source.to_synchronous)
      { when_reader_waits = `Carry_on_until_end_of_batch; when_waiting_done = `Read_more }
  in
  Mock_peer.expect
    t
    [%bin_reader: Test_helpers.Header.t]
    [%sexp_of: Test_helpers.Header.t];
  Mock_peer.expect
    t
    [%bin_reader: Protocol.Message.nat0_t]
    [%sexp_of: _ Protocol.Message.t];
  let%bind conn = Mock_peer.connect ~implementations t >>| Result.ok_exn in
  [%expect
    {|
    (Send (4411474 1 2 3 4 5 6 7 8 9 10 11))
    (Send
     (Metadata_v2
      ((identification ())
       (menu (((descriptions (((name unit) (version 1)))) (digests ())))))))
    |}];
  let%bind () =
    Time_source.advance_by_alarms_by time_source (Time_ns.Span.of_int_sec 5)
  in
  [%expect {| (Send Heartbeat) |}];
  Mock_peer.write_message
    t
    [%bin_writer: unit]
    (Query_v3
       { tag = Protocol.Rpc_tag.of_string "unit"
       ; version = 1
       ; id = Protocol.Query_id.of_int_exn 1
       ; metadata = None
       ; data = ()
       });
  let%bind () = Scheduler.yield_until_no_jobs_remain () in
  [%expect
    {|
    (Tracing_event
     ((event (Received Query)) (rpc ((name unit) (version 1))) (id 1)
      (payload_bytes 11)))
    |}];
  let%bind () =
    Time_source.advance_by_alarms_by time_source (Time_ns.Span.of_int_ms 500)
  in
  let close_finished =
    Async_rpc_kernel.Rpc.Connection.close
      ~wait_for_open_queries_timeout:Time_ns.Span.second
      conn
  in
  let%bind () = Scheduler.yield_until_no_jobs_remain () in
  [%expect
    {|
    (Send Close_started)
    (Close_started
     (("Connection closed by local side:" Rpc.Connection.close)
      (connection_description <created-directly>)))
    |}];
  let%bind () =
    Time_source.advance_by_alarms_by time_source (Time_ns.Span.of_int_ms 500)
  in
  let%bind () = close_finished in
  [%expect
    {|
    (Send
     (message
      ("00000000  08 01 01 00 00 01 00                              |.......|")))
    (Tracing_event
     ((event (Sent (Response Single_succeeded))) (rpc ((name unit) (version 1)))
      (id 1) (payload_bytes 1)))
    Close_writer
    Close_reader
    Close_finished
    |}];
  return ()
;;

let%expect_test "will not send close started message to old peer" =
  let t =
    Mock_peer.create
      { when_reader_waits = `Carry_on_until_end_of_batch; when_waiting_done = `Read_more }
  in
  Mock_peer.expect
    t
    [%bin_reader: Test_helpers.Header.t]
    [%sexp_of: Test_helpers.Header.t];
  Mock_peer.expect
    t
    [%bin_reader: Protocol.Message.nat0_t]
    [%sexp_of: _ Protocol.Message.t];
  let%bind conn = Mock_peer.connect ~send_handshake:(Some `v8) t >>| Result.ok_exn in
  [%expect
    {|
    (Send (4411474 1 2 3 4 5 6 7 8 9 10 11))
    (Send
     (Metadata_v2
      ((identification ()) (menu (((descriptions ()) (digests ())))))))
    |}];
  let%bind () = Scheduler.yield_until_no_jobs_remain () in
  [%expect {| (Send Heartbeat) |}];
  let%bind () =
    Async_rpc_kernel.Rpc.Connection.close
      ~wait_for_open_queries_timeout:Time_ns.Span.second
      conn
  in
  [%expect
    {|
    (Close_started
     (("Connection closed by local side:" Rpc.Connection.close)
      (connection_description <created-directly>)))
    Close_writer
    Close_reader
    Close_finished
    |}];
  return ()
;;
