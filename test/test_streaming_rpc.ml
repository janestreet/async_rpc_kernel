open! Core
open! Async

let plain_rpc =
  Rpc.Rpc.create
    ~name:"plain-rpc"
    ~version:1
    ~bin_query:[%bin_type_class: unit]
    ~bin_response:[%bin_type_class: unit]
    ~include_in_error_count:Only_on_exn
;;

let pipe_rpc =
  Rpc.Pipe_rpc.create
    ~name:"pipe-rpc"
    ~version:1
    ~bin_query:[%bin_type_class: unit]
    ~bin_response:[%bin_type_class: string]
    ~bin_error:[%bin_type_class: Nothing.t]
    ()
;;

let state_rpc =
  Rpc.State_rpc.create
    ~name:"state-rpc"
    ~version:1
    ~bin_query:[%bin_type_class: unit]
    ~bin_state:[%bin_type_class: string]
    ~bin_update:[%bin_type_class: string]
    ~bin_error:[%bin_type_class: Nothing.t]
    ()
;;

let read_and_print reader =
  Pipe.read reader >>| [%sexp_of: [ `Ok of string | `Eof ]] >>| print_s
;;

let max_message_size = 1_000
let overlarge_string = lazy (String.init (2 * max_message_size) ~f:(const 'a'))

let serve_with_client ~implementations =
  let implementations =
    Rpc.Implementations.create_exn
      ~implementations
      ~on_unknown_rpc:`Raise
      ~on_exception:Log_on_background_exn
  in
  let server_conn = Ivar.create () in
  let%bind server =
    Rpc.Connection.serve
      ~max_message_size
      ~implementations
      ~initial_connection_state:(fun (_ : [< Socket.Address.t ]) connection ->
        Ivar.fill_if_empty server_conn connection;
        connection)
      ~where_to_listen:Tcp.Where_to_listen.of_port_chosen_by_os
      ()
  in
  let%bind client_conn =
    Rpc.Connection.client
      ~max_message_size
      (Tcp.Where_to_connect.of_inet_address (Tcp.Server.listening_on_address server))
    >>| Or_error.of_exn_result
    >>| ok_exn
  in
  let%map server_conn = Ivar.read server_conn in
  upon (Rpc.Connection.close_finished server_conn) (fun () ->
    don't_wait_for (Tcp.Server.close server));
  client_conn
;;

let%expect_test "Receiving an invalid message via pipe rpc closes the pipe but doesn't \
                 close the connection"
  =
  let stream_writer = Ivar.create () in
  let%bind client_conn =
    serve_with_client
      ~implementations:
        [ Rpc.Pipe_rpc.implement_direct pipe_rpc (fun (_ : Rpc.Connection.t) () writer ->
            Ivar.fill_exn stream_writer writer;
            return (Ok ()))
        ; Rpc.Rpc.implement plain_rpc (fun (_ : Rpc.Connection.t) () -> return ())
        ]
  in
  let%bind reader, metadata = Rpc.Pipe_rpc.dispatch_exn pipe_rpc client_conn () in
  let%bind writer = Ivar.read stream_writer in
  let send_valid_message () =
    let (`Ok | `Closed) =
      Rpc.Pipe_rpc.Direct_stream_writer.write_without_pushback writer "foo"
    in
    ()
  in
  send_valid_message ();
  let%bind () = read_and_print reader in
  [%expect {| (Ok foo) |}];
  (* This message is invalid because it contains two (empty) strings, not just one. *)
  let invalid_message = Bigstring.of_string "\000\000" in
  let (`Ok | `Closed) =
    Rpc.Pipe_rpc.Direct_stream_writer.Expert.write_without_pushback
      writer
      ~buf:invalid_message
      ~pos:0
      ~len:2
  in
  (* Send a few more valid messages after the invalid message to ensure the client-side
     logic can handle them *)
  send_valid_message ();
  send_valid_message ();
  let%bind () = read_and_print reader in
  [%expect {| Eof |}];
  let%bind reason = Rpc.Pipe_rpc.close_reason metadata in
  Expect_test_helpers_core.print_s
    ([%sexp_of: Rpc.Pipe_close_reason.t] reason)
    ~hide_positions:true;
  [%expect
    {|
    (Error (
      Bin_io_exn (
        (location
         "client-side streaming_rpc response un-bin-io'ing to [bin_reader_update]")
        (exn (
          Failure
          "lib/async_rpc/kernel/src/rpc.ml:LINE:COL message length (1) did not match expected length (2)")))))
    |}];
  (* Send a [plain_rpc] to show that the connection isn't closed and ensure the abort
     message is transmitted. *)
  let%bind () = Rpc.Rpc.dispatch_exn plain_rpc client_conn () in
  Rpc.Pipe_rpc.Direct_stream_writer.Expert.write_without_pushback
    writer
    ~buf:invalid_message
    ~pos:0
    ~len:2
  |> [%sexp_of: [ `Ok | `Closed ]]
  |> print_s;
  [%expect {| Closed |}];
  [%test_result: bool] (Rpc.Pipe_rpc.Direct_stream_writer.is_closed writer) ~expect:true;
  (* Send another [plain_rpc] to show that the connection still isn't closed. *)
  let%bind () =
    Rpc.Rpc.dispatch plain_rpc client_conn () >>| [%sexp_of: unit Or_error.t] >>| print_s
  in
  [%expect {| (Ok ()) |}];
  return ()
;;

let%expect_test "Receiving an overly large message via pipe rpc closes the pipe but not \
                 the connection"
  =
  let stream_writer = Ivar.create () in
  let%bind client_conn =
    serve_with_client
      ~implementations:
        [ Rpc.Pipe_rpc.implement pipe_rpc (fun (_ : Rpc.Connection.t) () ->
            let reader, writer = Pipe.create () in
            Ivar.fill_exn stream_writer writer;
            return (Ok reader))
        ; Rpc.Rpc.implement plain_rpc (fun (_ : Rpc.Connection.t) () -> return ())
        ]
  in
  let%bind reader, metadata = Rpc.Pipe_rpc.dispatch_exn pipe_rpc client_conn () in
  let%bind writer = Ivar.read stream_writer in
  Pipe.write_without_pushback writer (Lazy.force overlarge_string);
  let%bind () = read_and_print reader in
  [%expect {| Eof |}];
  let%bind reason = Rpc.Pipe_rpc.close_reason metadata in
  print_s ([%sexp_of: Rpc.Pipe_close_reason.t] reason);
  [%expect
    {| (Error (Write_error (Message_too_big ((size 2018) (max_message_size 1000))))) |}];
  (* Send a [plain_rpc] to show that the connection isn't closed. *)
  Rpc.Rpc.dispatch_exn plain_rpc client_conn ()
;;

let%expect_test "Receiving an overly large message via state rpc initial update notifies \
                 the writer end that the pipe is closed"
  =
  let stream_writer = Ivar.create () in
  let%bind client_conn =
    serve_with_client
      ~implementations:
        [ Rpc.State_rpc.implement state_rpc (fun (_ : Rpc.Connection.t) () ->
            let reader, writer = Pipe.create () in
            Ivar.fill_exn stream_writer writer;
            return (Ok (Lazy.force overlarge_string, reader)))
        ; Rpc.Rpc.implement plain_rpc (fun (_ : Rpc.Connection.t) () -> return ())
        ]
  in
  let%bind () =
    Rpc.State_rpc.dispatch state_rpc client_conn ()
    >>| [%sexp_of: _ Or_error.t]
    >>| print_s
  in
  [%expect
    {|
    (Error
     ((rpc_error
       (Write_error (Message_too_big ((size 2013) (max_message_size 1000)))))
      (connection_description ("Client connected via TCP" 0.0.0.0:PORT))
      (rpc_name state-rpc) (rpc_version 1)))
    |}];
  let%bind writer = Ivar.read stream_writer in
  (* Dispatch another RPC along the connection to ensure the abort message for the
     streaming RPC makes it across the connection *)
  let%bind () = Rpc.Rpc.dispatch plain_rpc client_conn () >>| ok_exn in
  [%test_result: bool] (Rpc.Connection.is_closed client_conn) ~expect:false;
  [%test_result: bool] (Pipe.is_closed writer) ~expect:true;
  return ()
;;

let dispatch_with_close_reason_exn
  (pipe_rpc : ('query, 'response, 'error) Rpc.Pipe_rpc.t)
  client_connection
  (query : 'query)
  =
  match%map Rpc.Pipe_rpc.dispatch_with_close_reason pipe_rpc client_connection query with
  | Ok (Ok pipe) -> pipe
  | (_ : (('response, Error.t) Pipe_with_writer_error.t, 'error) result Or_error.t) ->
    raise_s [%message "Failed to dispatch pipe in test"]
;;

let pipe_with_writer_error_read_and_print pipe_with_writer_error =
  Pipe_with_writer_error.read pipe_with_writer_error
  >>| [%sexp_of: [ `Eof | `Ok of string ] Or_error.t]
  >>| print_s
;;

module%test [@name "[dispatch_with_close_reason]"] _ = struct
  let number_of_messages = 5
  let continue_bvar = Bvar.create ()

  let continue_read_and_print pipe_with_close_reason =
    Bvar.broadcast continue_bvar ();
    pipe_with_writer_error_read_and_print pipe_with_close_reason
  ;;

  let implementations =
    [ Rpc.Pipe_rpc.implement pipe_rpc (fun (_ : Rpc.Connection.t) () ->
        Pipe.create_reader ~close_on_exception:true (fun writer ->
          let%map () =
            List.init number_of_messages ~f:Fn.id
            |> Deferred.List.iter ~how:`Sequential ~f:(fun i ->
              let%bind () = Bvar.wait continue_bvar in
              Pipe.write_if_open writer (Int.to_string i))
          in
          Pipe.close writer)
        |> Deferred.Result.return)
    ]
  ;;

  let%expect_test "Close mid-stream" =
    let%bind client_conn = serve_with_client ~implementations in
    let%bind pipe_with_close_reason =
      dispatch_with_close_reason_exn pipe_rpc client_conn ()
    in
    let%bind () = continue_read_and_print pipe_with_close_reason in
    let%bind () = continue_read_and_print pipe_with_close_reason in
    let%bind () = Rpc.Connection.close client_conn in
    let%map () = continue_read_and_print pipe_with_close_reason in
    [%expect
      {|
      (Ok (Ok 0))
      (Ok (Ok 1))
      (Error
       (Connection_closed
        ((("Connection closed by local side:" Rpc.Connection.close)
          (connection_description ("Client connected via TCP" 0.0.0.0:PORT))))))
      |}]
  ;;

  let%expect_test "Clean close" =
    let%bind client_conn = serve_with_client ~implementations in
    let%bind pipe_with_close_reason =
      dispatch_with_close_reason_exn pipe_rpc client_conn ()
    in
    let%map () =
      List.init number_of_messages ~f:ignore
      |> Deferred.List.iter ~how:`Sequential ~f:(fun () ->
        continue_read_and_print pipe_with_close_reason)
    in
    [%expect
      {|
      (Ok (Ok 0))
      (Ok (Ok 1))
      (Ok (Ok 2))
      (Ok (Ok 3))
      (Ok (Ok 4))
      |}]
  ;;

  let%expect_test "Raise immediately" =
    let%bind client_conn =
      serve_with_client
        ~implementations:
          [ Rpc.Pipe_rpc.implement pipe_rpc (fun (_ : Rpc.Connection.t) () ->
              raise_s [%message "Immediate raise"])
          ]
    in
    Dynamic.set_root Backtrace.elide true;
    let%map () =
      let%map result = Rpc.Pipe_rpc.dispatch_with_close_reason pipe_rpc client_conn () in
      print_s [%message "" ~_:(Result.error result : Error.t option)]
    in
    Dynamic.set_root Backtrace.elide false;
    [%expect
      {|
      (((rpc_error
         (Uncaught_exn
          ((location "server-side pipe_rpc computation")
           (exn
            (monitor.ml.Error "Immediate raise" ("<backtrace elided in test>"))))))
        (connection_description ("Client connected via TCP" 0.0.0.0:PORT))
        (rpc_name pipe-rpc) (rpc_version 1)))
      |}]
  ;;
end

let%expect_test "Direct_stream_writer queues updates before [started], and writes them \
                 directly afterward"
  =
  let%bind client_conn =
    serve_with_client
      ~implementations:
        [ Rpc.Pipe_rpc.implement_direct pipe_rpc (fun connection () writer ->
            let total_bytes_sent () =
              Rpc.Connection.bytes_to_write connection
              + (Rpc.Connection.bytes_written connection |> Int63.to_int_exn)
            in
            let initial_bytes_sent = total_bytes_sent () in
            let (`Ok | `Closed) =
              Rpc.Pipe_rpc.Direct_stream_writer.write_without_pushback writer "foo"
            in
            assert (total_bytes_sent () = initial_bytes_sent);
            upon (Rpc.Pipe_rpc.Direct_stream_writer.started writer) (fun () ->
              let new_bytes_sent = total_bytes_sent () in
              assert (new_bytes_sent > initial_bytes_sent);
              let (`Ok | `Closed) =
                Rpc.Pipe_rpc.Direct_stream_writer.write_without_pushback writer "bar"
              in
              assert (total_bytes_sent () > new_bytes_sent);
              Rpc.Pipe_rpc.Direct_stream_writer.close writer);
            return (Ok ()))
        ]
  in
  let%bind reader, (_ : Rpc.Pipe_rpc.Metadata.t) =
    Rpc.Pipe_rpc.dispatch_exn pipe_rpc client_conn ()
  in
  let%bind () = Pipe.iter_without_pushback reader ~f:print_endline in
  [%expect
    {|
    foo
    bar
    |}];
  return ()
;;

module%test [@name "[leave_open_on_exception]"] _ = struct
  let dispatch_pipe_then_throw ~leave_open_on_exception =
    let raise_ivar = Ivar.create () in
    let pipe_writer = Ivar.create () in
    let%bind client_conn =
      serve_with_client
        ~implementations:
          [ Rpc.Pipe_rpc.implement
              pipe_rpc
              ~leave_open_on_exception
              (fun (_ : Rpc.Connection.t) () ->
                 Pipe.create_reader ~close_on_exception:false (fun writer ->
                   Ivar.fill_exn pipe_writer writer;
                   Pipe.write_without_pushback
                     writer
                     "Sending message to prove that the pipe was open initially";
                   don't_wait_for
                     (let%bind () = Ivar.read raise_ivar in
                      raise_s [%message "Raising in the background"]);
                   Deferred.never ())
                 |> Deferred.Result.return)
          ]
    in
    let%bind reader = dispatch_with_close_reason_exn pipe_rpc client_conn () in
    let%bind writer = Ivar.read pipe_writer in
    let%bind () = pipe_with_writer_error_read_and_print reader in
    Dynamic.set_root Backtrace.elide true;
    Ivar.fill_exn raise_ivar ();
    let%bind () = Scheduler.yield_until_no_jobs_remain () in
    Dynamic.set_root Backtrace.elide false;
    (* We always expect the [Rpc.Connection.t] to remain intact. *)
    [%test_result: bool] (Rpc.Connection.is_closed client_conn) ~expect:false;
    Pipe.write_without_pushback_if_open
      writer
      "Sending another message prove that the pipe isn't closed";
    pipe_with_writer_error_read_and_print reader
  ;;

  let%expect_test "[leave_open_on_exception:false]" =
    let%map () = dispatch_pipe_then_throw ~leave_open_on_exception:false in
    [%expect
      {|
      (Ok (Ok "Sending message to prove that the pipe was open initially"))
      1969-12-31 19:00:00.000000-05:00 Error ("Exception raised to [Monitor.try_with] that already returned.""This error was captured by a default handler in [Async.Log]."(exn(monitor.ml.Error"Raising in the background"("<backtrace elided in test>"))))
      (Error
       (Uncaught_exn
        (monitor.ml.Error "Raising in the background"
         ("<backtrace elided in test>"))))
      |}]
  ;;

  let%expect_test "[leave_open_on_exception:true]" =
    let%map () = dispatch_pipe_then_throw ~leave_open_on_exception:true in
    [%expect
      {|
      (Ok (Ok "Sending message to prove that the pipe was open initially"))
      1969-12-31 19:00:00.000000-05:00 Error ("Exception raised to [Monitor.try_with] that already returned.""This error was captured by a default handler in [Async.Log]."(exn(monitor.ml.Error"Raising in the background"("<backtrace elided in test>"))))
      (Ok (Ok "Sending another message prove that the pipe isn't closed"))
      |}]
  ;;

  let%expect_test "[leave_open_on_exception:false] transmits all messages sent before \
                   the exception was thrown"
    =
    let total_messages =
      let transfer_default_max_num_values_per_read =
        Async_rpc_kernel.Async_rpc_kernel_private.Transport.Writer
        .transfer_default_max_num_values_per_read
      in
      let batches = 10 in
      transfer_default_max_num_values_per_read * batches
    in
    let%bind client_conn =
      serve_with_client
        ~implementations:
          [ Rpc.Pipe_rpc.implement
              pipe_rpc
              ~leave_open_on_exception:false
              (fun (_ : Rpc.Connection.t) () ->
                 Pipe.create_reader ~close_on_exception:false (fun writer ->
                   let%bind () = Scheduler.yield () in
                   List.init total_messages ~f:Int.to_string
                   |> List.iter ~f:(Pipe.write_without_pushback writer);
                   failwith "exn")
                 |> Deferred.Result.return)
          ]
    in
    let messages_received = ref 0 in
    Dynamic.set_root Backtrace.elide true;
    let%bind reader = dispatch_with_close_reason_exn pipe_rpc client_conn () in
    let%bind result =
      Pipe_with_writer_error.iter_without_pushback reader ~f:(fun (_ : string) ->
        incr messages_received)
    in
    Dynamic.set_root Backtrace.elide false;
    [%test_result: int] !messages_received ~expect:total_messages;
    print_s [%sexp (result : unit Or_error.t)];
    [%expect
      {|
      1969-12-31 19:00:00.000000-05:00 Error ("Exception raised to [Monitor.try_with] that already returned.""This error was captured by a default handler in [Async.Log]."(exn(monitor.ml.Error(Failure exn)("<backtrace elided in test>"))))
      (Error
       (Uncaught_exn
        (monitor.ml.Error (Failure exn) ("<backtrace elided in test>"))))
      |}];
    return ()
  ;;

  let%expect_test "regression test: [leave_open_on_exception:false] is resilient to the \
                   case where a background exception is raised immediately after the \
                   implementation completes successfully, and doesn't leave the pipe \
                   hanging open"
    =
    let raise_after_schedule ~schedules =
      let raise_after_schedule_pipe_rpc =
        Rpc.Pipe_rpc.create
          ~name:"raise-after-schedule-pipe-rpc"
          ~version:1
          ~bin_query:[%bin_type_class: int]
          ~bin_response:[%bin_type_class: string]
          ~bin_error:[%bin_type_class: Nothing.t]
          ()
      in
      let pipe_ivar = Ivar.create () in
      let%bind client_conn =
        serve_with_client
          ~implementations:
            [ Rpc.Pipe_rpc.implement
                raise_after_schedule_pipe_rpc
                ~leave_open_on_exception:false
                (fun (_ : Rpc.Connection.t) depth ->
                   let reader, _ = Pipe.create () in
                   Ivar.fill_exn pipe_ivar reader;
                   let response = Ivar.create () in
                   Scheduler.schedule (fun () -> Ivar.fill_exn response (Ok reader));
                   let rec nested_schedule = function
                     | 0 -> failwith "sneaky nested exn"
                     | depth -> Scheduler.schedule (fun () -> nested_schedule (depth - 1))
                   in
                   nested_schedule depth;
                   Ivar.read response)
            ; Rpc.Rpc.implement plain_rpc (fun (_ : Rpc.Connection.t) () -> return ())
            ]
      in
      Dynamic.set_root Backtrace.elide true;
      let%bind () =
        match%map
          Rpc.Pipe_rpc.dispatch_with_close_reason
            raise_after_schedule_pipe_rpc
            client_conn
            schedules
        with
        | Ok (Ok pipe_with_writer_error) ->
          let peek_result = Pipe_with_writer_error.peek pipe_with_writer_error in
          print_s
            [%message
              "Dispatch succeeded" (peek_result : (string option, Error.t) result)]
        | Ok _ -> raise_s [%message "Expected error"]
        | Error error -> print_s [%message "Dispatch failed with error" (error : Error.t)]
      in
      (* Ensure the connection is still open *)
      let%bind () = Rpc.Rpc.dispatch_exn plain_rpc client_conn () in
      Dynamic.set_root Backtrace.elide false;
      let%map pipe = Ivar.read pipe_ivar in
      [%test_result: bool] (Pipe.is_closed pipe) ~expect:true
    in
    (* Through experimentation we found out that
       [schedules_required_for_implementation_to_complete_before_exn] schedules is the
       exact number to force the exception to be thrown immediately after the
       implementation returns. This is not stable to changes within the implementation
       flow, but it should be the case that the first dispatch fails, and the second one
       succeeds but then the pipe gets closed with an error close reason.

       We discussed the possibility of, instead of hardcoding this number, instead writing
       this test to exercise every number of schedules between e.g. 1 and 20 and asserting
       that their behaviour is correct. We decided against it for two reasons:
       1. We think the test would be noisier that way and it would be harder to validate
          that the test is actually exercising the difference in behaviour that we expect
          it to be testing.
       2. We think that this won't change frequently and shouldn't be a pain to adjust by
          hand if it does. If that assumption turns out false, we should re-evaluate. *)
    let schedules_required_for_implementation_to_complete_before_exn = 5 in
    let schedules_required_for_successful_response_before_exn =
      schedules_required_for_implementation_to_complete_before_exn + 2
    in
    (* This is effectively a synchronous exception as far as the monitor can tell. The
       pipe gets closed once the implementation completes. *)
    let%bind () =
      raise_after_schedule
        ~schedules:(schedules_required_for_implementation_to_complete_before_exn - 1)
    in
    [%expect
      {|
      ("Dispatch failed with error"
       (error
        ((rpc_error
          (Uncaught_exn
           ((location "server-side pipe_rpc computation")
            (exn
             (monitor.ml.Error (Failure "sneaky nested exn")
              ("<backtrace elided in test>"))))))
         (connection_description ("Client connected via TCP" 0.0.0.0:PORT))
         (rpc_name raise-after-schedule-pipe-rpc) (rpc_version 1))))
      |}];
    (* This is a background exn according to the monitor, but we process closing the pipe
       RPC before the successful response gets sent, so the client still gets "dispatch
       failed" *)
    let%bind () =
      raise_after_schedule
        ~schedules:schedules_required_for_implementation_to_complete_before_exn
    in
    [%expect
      {|
      1969-12-31 19:00:00.000000-05:00 Error ("Exception raised to [Monitor.try_with] that already returned.""This error was captured by a default handler in [Async.Log]."(exn(monitor.ml.Error(Failure"sneaky nested exn")("<backtrace elided in test>"))))
      ("Dispatch failed with error"
       (error
        ((rpc_error
          (Uncaught_exn
           (monitor.ml.Error (Failure "sneaky nested exn")
            ("<backtrace elided in test>"))))
         (connection_description ("Client connected via TCP" 0.0.0.0:PORT))
         (rpc_name raise-after-schedule-pipe-rpc) (rpc_version 1))))
      |}];
    (* If the exception takes long enough to show up, we send a successful response, and
       then only observe the pipe getting closed by peeking the response pipe. *)
    let%bind () =
      raise_after_schedule
        ~schedules:schedules_required_for_successful_response_before_exn
    in
    [%expect
      {|
      1969-12-31 19:00:00.000000-05:00 Error ("Exception raised to [Monitor.try_with] that already returned.""This error was captured by a default handler in [Async.Log]."(exn(monitor.ml.Error(Failure"sneaky nested exn")("<backtrace elided in test>"))))
      ("Dispatch succeeded"
       (peek_result
        (Error
         (Uncaught_exn
          (monitor.ml.Error (Failure "sneaky nested exn")
           ("<backtrace elided in test>"))))))
      |}];
    return ()
  ;;
end
