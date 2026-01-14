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

let yield_and_print_liveness ~server_conn ~client_conn =
  let%map () = Async_kernel_scheduler.yield_until_no_jobs_remain () in
  if Connection.is_closed server_conn |> not
  then
    print_s
      [%message
        "server" ~last_seen_alive:(Connection.last_seen_alive server_conn : Time_ns.t)];
  if Connection.is_closed client_conn |> not
  then
    print_s
      [%message
        "client" ~last_seen_alive:(Connection.last_seen_alive client_conn : Time_ns.t)]
;;

let print_emphasized str = print_endline ("-----" ^ str ^ "-----")

module%test [@name "Normal heartbeat timeouts"] _ = struct
  let test_connection_timeout heartbeat_timeout_style =
    let heartbeat_every = Time_ns.Span.of_sec 2. in
    let heartbeat_timeout = Time_ns.Span.of_sec 10. in
    let%bind ( `Server (server_time_source, server_conn)
             , `Client (client_time_source, client_conn) )
      =
      Test_helpers.setup_server_and_client_connection
        ~heartbeat_timeout
        ~heartbeat_every
        ~heartbeat_timeout_style
        ()
    in
    let yield_and_print_liveness () =
      yield_and_print_liveness ~server_conn ~client_conn
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

  let%expect_test "test connection with time_source <> wall_clock with \
                   time-between-heartbeats style timeouts"
    =
    let%bind () = test_connection_timeout Time_between_heartbeats_legacy in
    [%expect
      {|
      -----Advancing time by 0 to init-----
      ("received heartbeat"(now"1970-01-01 00:00:00Z")(description server))
      ("received heartbeat"(now"1970-01-01 00:00:00Z")(description client))
      (server (last_seen_alive "1970-01-01 00:00:00Z"))
      (client (last_seen_alive "1970-01-01 00:00:00Z"))
      -----Advancing time by heartbeat_every to show a heartbeat-----
      ("received heartbeat"(now"1970-01-01 00:00:02Z")(description client))
      ("received heartbeat"(now"1970-01-01 00:00:02Z")(description server))
      (server (last_seen_alive "1970-01-01 00:00:02Z"))
      (client (last_seen_alive "1970-01-01 00:00:02Z"))
      -----Advancing only server time by heartbeat_timeout to show heartbeats just before a timeout-----
      ("received heartbeat"(now"1970-01-01 00:00:02Z")(description client))
      ("received heartbeat"(now"1970-01-01 00:00:02Z")(description client))
      ("received heartbeat"(now"1970-01-01 00:00:02Z")(description client))
      ("received heartbeat"(now"1970-01-01 00:00:02Z")(description client))
      ("received heartbeat"(now"1970-01-01 00:00:02Z")(description client))
      (server (last_seen_alive "1970-01-01 00:00:02Z"))
      (client (last_seen_alive "1970-01-01 00:00:02Z"))
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
      |}];
    return ()
  ;;

  let%expect_test "test connection with time_source <> wall_clock, showing that \
                   number-of-heartbeats style timeouts behave like \
                   time-between-heartbeats style timeouts when client and server \
                   heartbeat timeout are the same"
    =
    let%bind () = test_connection_timeout Time_between_heartbeats_legacy in
    let time_based_timeout = Expect_test_helpers_core.expect_test_output () in
    let%bind () = test_connection_timeout Number_of_heartbeats in
    let count_based_timeout = Expect_test_helpers_core.expect_test_output () in
    Expect_test_patdiff.print_patdiff ~context:1 time_based_timeout count_based_timeout;
    (* The diff is just a changed message reflecting that are using a different style of
       timeout, but otherwise the timeout occurs at the same time in both styles *)
    [%expect
      {|
      === DIFF HUNK ===
            ("Connection closed by local side:"
      -|     "No heartbeats received for 10s. Last seen at: 1969-12-31 19:00:02-05:00, now: 1969-12-31 19:00:14-05:00.")
      +|     "No heartbeats received in the time that we sent 5 heartbeats and were about to send one more. Last seen at: 1969-12-31 19:00:02-05:00, now: 1969-12-31 19:00:14-05:00.")
            (connection_description server))))
      === DIFF HUNK ===
            ("Connection closed by remote side:"
      -|     "No heartbeats received for 10s. Last seen at: 1969-12-31 19:00:02-05:00, now: 1969-12-31 19:00:14-05:00.")
      +|     "No heartbeats received in the time that we sent 5 heartbeats and were about to send one more. Last seen at: 1969-12-31 19:00:02-05:00, now: 1969-12-31 19:00:14-05:00.")
            (connection_description client))))
      |}];
    return ()
  ;;
end

module%test [@name "skewed heartbeat timeouts"] _ = struct
  (* We want to test connections where
   * server has a much longer timeout than
   * the client * client does sychronous work longer than their own timeout
   * synchronous work is shorter than the server timeout
  *)

  let client_heartbeat_timeout = Time_ns.Span.of_sec 15.
  let server_heartbeat_timeout = Time_ns.Span.of_sec 60.
  let heartbeat_every = Time_ns.Span.of_sec 4.

  let test_connection_timeout_when_server_timeout_is_much_longer heartbeat_timeout_style =
    let%bind ( `Server (server_time_source, server_conn)
             , `Client (client_time_source, client_conn) )
      =
      Test_helpers.setup_server_and_client_connection
        ~heartbeat_timeout:client_heartbeat_timeout
        ~heartbeat_every
        ~heartbeat_timeout_style
        ~server_heartbeat_timeout
        ()
    in
    let yield_and_print_liveness () =
      yield_and_print_liveness ~server_conn ~client_conn
    in
    print_emphasized "Advancing time by 0 to init";
    advance_by_alarms_by client_time_source Time_ns.Span.zero;
    advance_by_alarms_by server_time_source Time_ns.Span.zero;
    let%bind () = yield_and_print_liveness () in
    (* Using advance_directly_by only on client_time_source mirrors the async behavior we
       would see when a client does long synchronous computation or has long async cycles
       but the server is still well behaved *)
    let span_25s = Time_ns.Span.of_sec 25. in
    print_emphasized
      [%string
        {|Advancing client time by %{span_25s#Time_ns.Span} to demonstrate synchronous work on the client longer than its heartbeat timeout.
     We expect %{Time_ns.Span.div span_25s heartbeat_every#Int63} heartbeats from the server.|}];
    Synchronous_time_source.advance_directly_by client_time_source span_25s |> ok_exn;
    advance_by_alarms_by server_time_source (Time_ns.Span.of_sec 25.);
    let%bind () = yield_and_print_liveness () in
    (* With count-based heartbeats, the number of heartbeats we allow is
       ceil(heartbeat_timeout / heartbeat_every). Which is 4 in this case for the client.
       This means that our timeout in practice will be between heartbeat timeout and
       heartbeat_timeout rounded up to the next closest multiple of heartbeat_every.
       Despite the heartbeat timeout being 15s, we can wait for 18s here with no timeout,
       but then we do get timed out after waiting another 1s later (total of 25+18+1s=44s
       (a multiple of 4s, the heartbeat_every value) from the start of the test) *)
    let span_18s = Time_ns.Span.of_sec 18. in
    print_emphasized
      [%string
        {|Advancing client time by %{span_18s#Time_ns.Span} to show that with count-based timeouts we don't timeout yet.|}];
    advance_by_alarms_by client_time_source span_18s;
    let%bind () = yield_and_print_liveness () in
    print_emphasized "Advancing only client time by 1s more";
    advance_by_alarms_by client_time_source (Time_ns.Span.of_sec 1.);
    let%bind () = yield_and_print_liveness () in
    Deferred.all_unit
      [ Connection.close_finished server_conn; Connection.close_finished client_conn ]
  ;;

  let%expect_test "old time-based heartbeat timeouts" =
    let%bind () =
      test_connection_timeout_when_server_timeout_is_much_longer
        Time_between_heartbeats_legacy
    in
    [%expect
      {|
      -----Advancing time by 0 to init-----
      ("received heartbeat"(now"1970-01-01 00:00:00Z")(description server))
      ("received heartbeat"(now"1970-01-01 00:00:00Z")(description client))
      (server (last_seen_alive "1970-01-01 00:00:00Z"))
      (client (last_seen_alive "1970-01-01 00:00:00Z"))
      -----Advancing client time by 25s to demonstrate synchronous work on the client longer than its heartbeat timeout.
           We expect 6 heartbeats from the server.-----
      ("received heartbeat"(now"1970-01-01 00:00:25Z")(description client))
      ("connection closed"
        (now         "1970-01-01 00:00:25Z")
        (description client)
        (reason (
          ("Connection closed by local side:"
           "No heartbeats received for 15s. Last seen at: 1969-12-31 19:00-05:00, now: 1969-12-31 19:00:25-05:00.")
          (connection_description client))))
      ("received heartbeat"(now"1970-01-01 00:00:25Z")(description client))
      ("connection closed"
        (now         "1970-01-01 00:00:25Z")
        (description server)
        (reason (
          ("Connection closed by remote side:"
           "No heartbeats received for 15s. Last seen at: 1969-12-31 19:00-05:00, now: 1969-12-31 19:00:25-05:00.")
          (connection_description server))))
      ("received heartbeat"(now"1970-01-01 00:00:25Z")(description client))
      ("received heartbeat"(now"1970-01-01 00:00:25Z")(description client))
      ("received heartbeat"(now"1970-01-01 00:00:25Z")(description client))
      ("received heartbeat"(now"1970-01-01 00:00:25Z")(description client))
      -----Advancing client time by 18s to show that with count-based timeouts we don't timeout yet.-----
      -----Advancing only client time by 1s more-----
      |}];
    return ()
  ;;

  let%expect_test "new count-based heartbeat timeouts" =
    let%bind () =
      test_connection_timeout_when_server_timeout_is_much_longer Number_of_heartbeats
    in
    [%expect
      {|
      -----Advancing time by 0 to init-----
      ("received heartbeat"(now"1970-01-01 00:00:00Z")(description server))
      ("received heartbeat"(now"1970-01-01 00:00:00Z")(description client))
      (server (last_seen_alive "1970-01-01 00:00:00Z"))
      (client (last_seen_alive "1970-01-01 00:00:00Z"))
      -----Advancing client time by 25s to demonstrate synchronous work on the client longer than its heartbeat timeout.
           We expect 6 heartbeats from the server.-----
      ("received heartbeat"(now"1970-01-01 00:00:25Z")(description server))
      ("received heartbeat"(now"1970-01-01 00:00:25Z")(description client))
      ("received heartbeat"(now"1970-01-01 00:00:25Z")(description client))
      ("received heartbeat"(now"1970-01-01 00:00:25Z")(description client))
      ("received heartbeat"(now"1970-01-01 00:00:25Z")(description client))
      ("received heartbeat"(now"1970-01-01 00:00:25Z")(description client))
      ("received heartbeat"(now"1970-01-01 00:00:25Z")(description client))
      (server (last_seen_alive "1970-01-01 00:00:25Z"))
      (client (last_seen_alive "1970-01-01 00:00:25Z"))
      -----Advancing client time by 18s to show that with count-based timeouts we don't timeout yet.-----
      ("received heartbeat"(now"1970-01-01 00:00:25Z")(description server))
      ("received heartbeat"(now"1970-01-01 00:00:25Z")(description server))
      ("received heartbeat"(now"1970-01-01 00:00:25Z")(description server))
      ("received heartbeat"(now"1970-01-01 00:00:25Z")(description server))
      (server (last_seen_alive "1970-01-01 00:00:25Z"))
      (client (last_seen_alive "1970-01-01 00:00:25Z"))
      -----Advancing only client time by 1s more-----
      ("connection closed"
        (now         "1970-01-01 00:00:44Z")
        (description client)
        (reason (
          ("Connection closed by local side:"
           "No heartbeats received in the time that we sent 4 heartbeats and were about to send one more. Last seen at: 1969-12-31 19:00:25-05:00, now: 1969-12-31 19:00:44-05:00.")
          (connection_description client))))
      ("connection closed"
        (now         "1970-01-01 00:00:25Z")
        (description server)
        (reason (
          ("Connection closed by remote side:"
           "No heartbeats received in the time that we sent 4 heartbeats and were about to send one more. Last seen at: 1969-12-31 19:00:25-05:00, now: 1969-12-31 19:00:44-05:00.")
          (connection_description server))))
      |}];
    return ()
  ;;
end
