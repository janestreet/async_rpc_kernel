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
    (Send (4411474 1 2 3 4 5 6 7))
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
  let%bind (conn : Rpc.Connection.t) = Mock_peer.connect t >>| Result.ok_exn in
  let%bind () = Rpc.Connection.close_finished conn in
  [%expect
    {|
    (Send
     (message
      ("00000000  08 fd 52 50 43 00 01 02  03 04 05 06 07           |..RPC........|")))
    (Send
     (message
      ("00000000  04 00 01 00                                       |....|")))
    Close_writer
    Close_reader
    (Close_started
     (("Connection closed by remote side:" "immediate close")
      (connection_description <created-directly>)))
    Close_finished
    |}];
  return ()
;;
