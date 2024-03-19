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
    (Send (4411474 1 2 3))
    (Send (Metadata ((identification ()) (menu (())))))
    |}];
  let%bind () = Scheduler.yield_until_no_jobs_remain () in
  [%expect {| (Send Heartbeat) |}];
  let%bind () = Async_rpc_kernel.Rpc.Connection.close conn in
  [%expect
    {|
    (Close_started Rpc.Connection.close)
    Close_writer
    Close_reader
    Close_finished
    |}];
  return ()
;;
