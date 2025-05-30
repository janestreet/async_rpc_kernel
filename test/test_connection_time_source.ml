open! Core
open! Async
open! Async_rpc_kernel
open! Async_rpc_kernel_private
open! Expect_test_helpers_core
open! Expect_test_helpers_async
module Time_ns = Core.Core_private.Time_ns_alternate_sexp

let sec = Time_ns.Span.of_sec

let advance_by_span time_source span =
  let to_ = Time_ns.add (Synchronous_time_source.now time_source) span in
  Synchronous_time_source.advance_by_alarms time_source ~to_ |> ok_exn
;;

let heartbeat_every = sec 2.
let heartbeat_timeout = sec 10.
let yield () = Async_kernel_scheduler.yield_until_no_jobs_remain ()

let%expect_test "test connection with time_source <> wall_clock" =
  let%bind ( `Server (server_time_source, server_conn)
           , `Client (client_time_source, client_conn) )
    =
    Test_helpers.setup_server_and_client_connection ~heartbeat_timeout ~heartbeat_every
  in
  [%expect {| |}];
  let print_liveness conn =
    print_s [%message "" ~last_seen_alive:(Connection.last_seen_alive conn : Time_ns.t)]
  in
  advance_by_span client_time_source Time_ns.Span.zero;
  advance_by_span server_time_source Time_ns.Span.zero;
  let%bind () = yield () in
  [%expect
    {|
    ("received heartbeat"
      (now         "1970-01-01 00:00:00Z")
      (description server))
    ("received heartbeat"
      (now         "1970-01-01 00:00:00Z")
      (description client))
    |}];
  print_liveness server_conn;
  print_liveness client_conn;
  [%expect
    {|
    (last_seen_alive "1970-01-01 00:00:00Z")
    (last_seen_alive "1970-01-01 00:00:00Z")
    |}];
  advance_by_span server_time_source heartbeat_every;
  advance_by_span client_time_source heartbeat_every;
  let%bind () = yield () in
  [%expect
    {|
    ("received heartbeat"
      (now         "1970-01-01 00:00:02Z")
      (description client))
    ("received heartbeat"
      (now         "1970-01-01 00:00:02Z")
      (description server))
    |}];
  print_liveness server_conn;
  print_liveness client_conn;
  [%expect
    {|
    (last_seen_alive "1970-01-01 00:00:02Z")
    (last_seen_alive "1970-01-01 00:00:02Z")
    |}];
  advance_by_span server_time_source heartbeat_timeout;
  let%bind () = yield () in
  [%expect
    {|
    ("received heartbeat"
      (now         "1970-01-01 00:00:02Z")
      (description client))
    ("received heartbeat"
      (now         "1970-01-01 00:00:02Z")
      (description client))
    ("received heartbeat"
      (now         "1970-01-01 00:00:02Z")
      (description client))
    ("received heartbeat"
      (now         "1970-01-01 00:00:02Z")
      (description client))
    ("received heartbeat"
      (now         "1970-01-01 00:00:02Z")
      (description client))
    |}];
  print_liveness server_conn;
  print_liveness client_conn;
  [%expect
    {|
    (last_seen_alive "1970-01-01 00:00:02Z")
    (last_seen_alive "1970-01-01 00:00:02Z")
    |}];
  advance_by_span server_time_source heartbeat_every;
  let%bind () = yield () in
  [%expect
    {|
    ("connection closed"
      (now         "1970-01-01 00:00:14Z")
      (description server)
      (reason (
        ("Connection closed by local side:"
         "No heartbeats received for 10s. Last seen at: 1969-12-31 19:00:02-05:00, now: 1969-12-31 19:00:14-05:00.")
        (connection_description server))))
    ("connection closed"
      (now         "1970-01-01 00:00:02Z")
      (description client)
      (reason (
        ("Connection closed by remote side:"
         "No heartbeats received for 10s. Last seen at: 1969-12-31 19:00:02-05:00, now: 1969-12-31 19:00:14-05:00.")
        (connection_description client))))
    |}];
  print_liveness server_conn;
  print_liveness client_conn;
  [%expect
    {|
    (last_seen_alive "1970-01-01 00:00:02Z")
    (last_seen_alive "1970-01-01 00:00:02Z")
    |}];
  Deferred.all_unit
    [ Connection.close_finished server_conn; Connection.close_finished client_conn ]
;;
