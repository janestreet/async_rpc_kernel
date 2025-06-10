open! Core
open! Async
open! Async_rpc_kernel
open! Async_rpc_kernel_private
open! Expect_test_helpers_core
open! Expect_test_helpers_async
module Time_ns = Core.Core_private.Time_ns_alternate_sexp

let advance_by_alarms_by time_source span =
  Synchronous_time_source.advance_by_alarms_by time_source span |> ok_exn
;;

let test_connection_with_time_source_not_equal_to_wall_clock () =
  let print_emphasized str = print_endline ("-----" ^ str ^ "-----") in
  let heartbeat_every = Time_ns.Span.of_sec 2. in
  let heartbeat_timeout = Time_ns.Span.of_sec 10. in
  let%bind ( `Server (server_time_source, server_conn)
           , `Client (client_time_source, client_conn) )
    =
    Test_helpers.setup_server_and_client_connection ~heartbeat_timeout ~heartbeat_every
  in
  let yield_and_print_liveness () =
    let%map () = Async_kernel_scheduler.yield_until_no_jobs_remain () in
    print_s
      [%message "" ~last_seen_alive:(Connection.last_seen_alive server_conn : Time_ns.t)];
    print_s
      [%message "" ~last_seen_alive:(Connection.last_seen_alive client_conn : Time_ns.t)]
  in
  print_emphasized "Advancing time by 0 to init";
  advance_by_alarms_by client_time_source Time_ns.Span.zero;
  advance_by_alarms_by server_time_source Time_ns.Span.zero;
  let%bind () = yield_and_print_liveness () in
  print_emphasized "Advancing time by heartbeat_every to show a heartbeat";
  advance_by_alarms_by server_time_source heartbeat_every;
  advance_by_alarms_by client_time_source heartbeat_every;
  let%bind () = yield_and_print_liveness () in
  print_emphasized
    "Advancing only server time by heartbeat_timeout to show heartbeats just before a \
     timeout";
  advance_by_alarms_by server_time_source heartbeat_timeout;
  let%bind () = yield_and_print_liveness () in
  print_emphasized
    "Advancing only server time by another heartbeat_every to force one more heartbeat \
     and cause a timeout";
  advance_by_alarms_by server_time_source heartbeat_every;
  let%bind () = yield_and_print_liveness () in
  Deferred.all_unit
    [ Connection.close_finished server_conn; Connection.close_finished client_conn ]
;;

let%expect_test "test connection with time_source <> wall_clock" =
  let%bind () = test_connection_with_time_source_not_equal_to_wall_clock () in
  [%expect
    {|
    -----Advancing time by 0 to init-----
    ("received heartbeat"(now"1970-01-01 00:00:00Z")(description server))
    ("received heartbeat"(now"1970-01-01 00:00:00Z")(description client))
    (last_seen_alive "1970-01-01 00:00:00Z")
    (last_seen_alive "1970-01-01 00:00:00Z")
    -----Advancing time by heartbeat_every to show a heartbeat-----
    ("received heartbeat"(now"1970-01-01 00:00:02Z")(description client))
    ("received heartbeat"(now"1970-01-01 00:00:02Z")(description server))
    (last_seen_alive "1970-01-01 00:00:02Z")
    (last_seen_alive "1970-01-01 00:00:02Z")
    -----Advancing only server time by heartbeat_timeout to show heartbeats just before a timeout-----
    ("received heartbeat"(now"1970-01-01 00:00:02Z")(description client))
    ("received heartbeat"(now"1970-01-01 00:00:02Z")(description client))
    ("received heartbeat"(now"1970-01-01 00:00:02Z")(description client))
    ("received heartbeat"(now"1970-01-01 00:00:02Z")(description client))
    ("received heartbeat"(now"1970-01-01 00:00:02Z")(description client))
    (last_seen_alive "1970-01-01 00:00:02Z")
    (last_seen_alive "1970-01-01 00:00:02Z")
    -----Advancing only server time by another heartbeat_every to force one more heartbeat and cause a timeout-----
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
    (last_seen_alive "1970-01-01 00:00:02Z")
    (last_seen_alive "1970-01-01 00:00:02Z")
    |}];
  return ()
;;
