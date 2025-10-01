open! Core
open! Async
open Import

module Sleep = struct
  let rpc =
    Rpc.Rpc.create
      ~name:"sleep"
      ~version:1
      ~bin_query:[%bin_type_class: Time_ns.Span.t]
      ~bin_response:[%bin_type_class: unit]
      ~include_in_error_count:Only_on_exn
  ;;

  let impl did_see_query time_source =
    Rpc.Rpc.implement rpc (fun _ span ->
      assert (Mvar.is_empty did_see_query);
      Mvar.set did_see_query ();
      Time_source.after time_source span)
  ;;
end

module Sleep_expert = struct
  let rpc =
    Rpc.Rpc.create
      ~name:"sleep_expert"
      ~version:1
      ~bin_query:[%bin_type_class: Time_ns.Span.t]
      ~bin_response:[%bin_type_class: unit]
      ~include_in_error_count:Only_on_exn
  ;;

  let impl did_see_query time_source =
    Rpc.Rpc.Expert.implement rpc (fun _ responder buf ~pos ~len ->
      assert (Mvar.is_empty did_see_query);
      Mvar.set did_see_query ();
      let pos_ref = ref pos in
      let duration = Time_ns.Span.bin_read_t buf ~pos_ref in
      assert (!pos_ref = pos + len);
      let%map () = Time_source.after time_source duration in
      Rpc.Rpc.Expert.Responder.write_bin_prot responder [%bin_writer: unit] ();
      Rpc.Rpc.Expert.Replied)
  ;;
end

let run_test ~f:test =
  let time_source = Time_source.create ~now:Time_ns.epoch () in
  let did_see_query = Mvar.create () in
  Test_helpers.with_rpc_server_connection
    ~time_source
    ~server_implementations:
      [ Sleep.impl did_see_query time_source
      ; Sleep_expert.impl did_see_query time_source
      ]
    ()
    ~server_header:Test_helpers.Header.latest
    ~client_header:Test_helpers.Header.latest
    ~f:(fun ~client ~server ~s_to_c:_ ~c_to_s:_ ->
      let dispatch ?(wait_for_did_see_query = true) rpc query =
        let result = Rpc.Rpc.dispatch rpc client query in
        (* Wait for the server to see the query since it needs to do some real IO. *)
        let%bind () =
          if wait_for_did_see_query then Mvar.take did_see_query else return ()
        in
        return result
      in
      let sleep span =
        Time_source.advance_by_alarms_by
          time_source
          (Time_ns.Span.of_string span)
          ~wait_for:(Scheduler.yield_until_no_jobs_remain ~may_return_immediately:false)
      in
      test ~dispatch ~sleep ~server)
;;

let%expect_test "RPC fails when connection is closed" =
  run_test ~f:(fun ~dispatch ~sleep ~server ->
    let%bind rpc_result = dispatch Sleep.rpc Time_ns.Span.hour in
    let%bind () = sleep "1s" in
    let%bind () = Rpc.Connection.close server
    and rpc_result in
    print_s [%sexp (rpc_result : unit Or_error.t)];
    [%expect
      {|
      (Error
       ((rpc_error
         (Connection_closed
          (("EOF or connection closed"
            (connection_description ("Client connected via TCP" (localhost PORT)))))))
        (connection_description ("Client connected via TCP" (localhost PORT)))
        (rpc_name sleep) (rpc_version 1)))
      |}];
    return ())
;;

let%expect_test "Closing and waiting for open queries" =
  run_test ~f:(fun ~dispatch ~sleep ~server ->
    let%bind rpc_result = dispatch Sleep.rpc (Time_ns.Span.of_int_sec 10) in
    let%bind () = sleep "1s" in
    let%bind () =
      Rpc.Connection.close ~wait_for_open_queries_timeout:Time_ns.Span.minute server
    and () = sleep "9s"
    and rpc_result in
    print_s [%sexp (rpc_result : unit Or_error.t)];
    [%expect {| (Ok ()) |}];
    return ())
;;

let%expect_test "Can't send new queries while the connection is closing" =
  run_test ~f:(fun ~dispatch ~sleep ~server ->
    (* Send one RPC before closing the connection, which we expect to succeed. *)
    let%bind rpc_before_close = dispatch Sleep.rpc (Time_ns.Span.of_int_sec 10) in
    let%bind () = sleep "1s" in
    let closed =
      Rpc.Connection.close ~wait_for_open_queries_timeout:Time_ns.Span.minute server
    in
    (* Sleep for 5s. Since the original RPC is still open, the connection should not have
       finished closing. *)
    let%bind () = sleep "5s" in
    print_s [%message (Deferred.is_determined closed : bool)];
    [%expect {| ("Deferred.is_determined closed" false) |}];
    (* Send a second RPC, which the server should drop as it's in the process of closing. *)
    let%bind rpc_after_close =
      dispatch ~wait_for_did_see_query:false Sleep.rpc Time_ns.Span.second
    in
    (* Wait for the original RPC to finish and observe the server being closed. *)
    let%bind () = sleep "5s"
    and () = closed
    and rpc_before_close
    and rpc_after_close in
    print_s [%sexp (rpc_before_close : unit Or_error.t)];
    [%expect {| (Ok ()) |}];
    print_s [%sexp (rpc_after_close : unit Or_error.t)];
    [%expect
      {|
      (Error
       ((rpc_error
         (Connection_closed
          (("EOF or connection closed"
            (connection_description ("Client connected via TCP" (localhost PORT)))))))
        (connection_description ("Client connected via TCP" (localhost PORT)))
        (rpc_name sleep) (rpc_version 1)))
      |}];
    return ())
;;

let%expect_test "Waiting for open queries can timeout" =
  run_test ~f:(fun ~dispatch ~sleep ~server ->
    let%bind rpc_result = dispatch Sleep.rpc (Time_ns.Span.of_int_sec 10) in
    let%bind () = sleep "1s" in
    let closed =
      Rpc.Connection.close ~wait_for_open_queries_timeout:Time_ns.Span.second server
    in
    let%bind () = sleep "500ms" in
    print_s [%message (Deferred.is_determined closed : bool)];
    [%expect {| ("Deferred.is_determined closed" false) |}];
    let%bind () = sleep "500ms"
    and () = closed
    and rpc_result in
    print_s [%sexp (rpc_result : unit Or_error.t)];
    [%expect
      {|
      (Error
       ((rpc_error
         (Connection_closed
          (("EOF or connection closed"
            (connection_description ("Client connected via TCP" (localhost PORT)))))))
        (connection_description ("Client connected via TCP" (localhost PORT)))
        (rpc_name sleep) (rpc_version 1)))
      |}];
    return ())
;;

let%expect_test "Multiple open queries, some of which timeout" =
  run_test ~f:(fun ~dispatch ~sleep ~server ->
    let%bind rpc_two_seconds = dispatch Sleep.rpc (Time_ns.Span.of_int_sec 2) in
    let%bind rpc_five_seconds = dispatch Sleep.rpc (Time_ns.Span.of_int_sec 5) in
    let%bind rpc_ten_seconds = dispatch Sleep.rpc (Time_ns.Span.of_int_sec 10) in
    let%bind () = sleep "1s" in
    let%bind () =
      Rpc.Connection.close
        ~wait_for_open_queries_timeout:(Time_ns.Span.of_int_sec 5)
        server
    and () = sleep "5s"
    and rpc_two_seconds
    and rpc_five_seconds
    and rpc_ten_seconds in
    print_s [%sexp (rpc_two_seconds : unit Or_error.t)];
    [%expect {| (Ok ()) |}];
    print_s [%sexp (rpc_five_seconds : unit Or_error.t)];
    [%expect {| (Ok ()) |}];
    print_s [%sexp (rpc_ten_seconds : unit Or_error.t)];
    [%expect
      {|
      (Error
       ((rpc_error
         (Connection_closed
          (("EOF or connection closed"
            (connection_description ("Client connected via TCP" (localhost PORT)))))))
        (connection_description ("Client connected via TCP" (localhost PORT)))
        (rpc_name sleep) (rpc_version 1)))
      |}];
    return ())
;;

let%expect_test "Expert RPC with graceful close" =
  run_test ~f:(fun ~dispatch ~sleep ~server ->
    let%bind rpc_result = dispatch Sleep_expert.rpc (Time_ns.Span.of_int_sec 10) in
    let%bind () = sleep "1s" in
    let%bind () =
      Rpc.Connection.close ~wait_for_open_queries_timeout:Time_ns.Span.minute server
    and () = sleep "10s"
    and rpc_result in
    print_s [%sexp (rpc_result : unit Or_error.t)];
    [%expect {| (Ok ()) |}];
    return ())
;;

let%expect_test "Expert RPC can timeout during graceful close" =
  run_test ~f:(fun ~dispatch ~sleep ~server ->
    let%bind rpc_result = dispatch Sleep_expert.rpc (Time_ns.Span.of_int_sec 10) in
    let%bind () = sleep "1s" in
    let closed =
      Rpc.Connection.close
        ~wait_for_open_queries_timeout:(Time_ns.Span.of_int_sec 5)
        server
    in
    let%bind () = sleep "5s" in
    let%bind () = closed in
    let%bind rpc_result in
    print_s [%sexp (rpc_result : unit Or_error.t)];
    [%expect
      {|
      (Error
       ((rpc_error
         (Connection_closed
          (("EOF or connection closed"
            (connection_description ("Client connected via TCP" (localhost PORT)))))))
        (connection_description ("Client connected via TCP" (localhost PORT)))
        (rpc_name sleep_expert) (rpc_version 1)))
      |}];
    return ())
;;

let%expect_test "Mixed regular and expert RPCs with graceful close" =
  run_test ~f:(fun ~dispatch ~sleep ~server ->
    let%bind regular_rpc_result = dispatch Sleep.rpc (Time_ns.Span.of_int_sec 5) in
    let%bind expert_rpc_result = dispatch Sleep_expert.rpc (Time_ns.Span.of_int_sec 10) in
    let%bind () = sleep "1s" in
    let%bind () =
      Rpc.Connection.close ~wait_for_open_queries_timeout:Time_ns.Span.minute server
    and () = sleep "10s"
    and regular_rpc_result
    and expert_rpc_result in
    print_s [%sexp (regular_rpc_result : unit Or_error.t)];
    [%expect {| (Ok ()) |}];
    print_s [%sexp (expert_rpc_result : unit Or_error.t)];
    [%expect {| (Ok ()) |}];
    return ())
;;
