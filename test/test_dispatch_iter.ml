open! Core
open! Async

let%expect_test "test Pipe_rpc.dispatch_iter" =
  let%map () =
    Test_helpers.with_rpc_server_connection
      ~server_header:Test_helpers.Header.v2
      ~client_header:Test_helpers.Header.v2
      ~f:(fun ~client ~server:_ ~s_to_c:_ ~c_to_s:_ ->
      let closed = Ivar.create () in
      let closed_and_waited = ref [ Ivar.read closed ] in
      let got_response = Ivar.create () in
      let payload = Bigstring.of_string "hello" in
      let%bind (_ : Rpc.Pipe_rpc.Id.t) =
        Rpc.Pipe_rpc.dispatch_iter
          Test_helpers.pipe_rpc
          client
          payload
          ~f:
            (fun
              (message : Bigstring.t Rpc.Pipe_rpc.Pipe_message.t)
              : Rpc.Pipe_rpc.Pipe_response.t
              ->
          (match message with
           | Update message ->
             let d =
               let%map () = Ivar.read got_response in
               print_s [%sexp (message : Bigstring.t)]
             in
             closed_and_waited := d :: !closed_and_waited;
             Wait d
           | Closed kind ->
             Ivar.fill_exn closed ();
             (match kind with
              | `By_remote_side -> Continue
              | `Error error -> Error.raise error)))
        >>| Or_error.join
        >>| Or_error.ok_exn
      in
      Ivar.fill_exn got_response ();
      print_endline "dispatch resolved";
      Deferred.all_unit !closed_and_waited)
  in
  [%expect {|
    dispatch resolved
    response
    response
    |}]
;;

let%expect_test "test Pipe_rpc.Expert.dispatch_iter" =
  let%map () =
    Test_helpers.with_rpc_server_connection
      ~server_header:Test_helpers.Header.v2
      ~client_header:Test_helpers.Header.v2
      ~f:(fun ~client ~server:_ ~s_to_c:_ ~c_to_s:_ ->
      let closed = Ivar.create () in
      let closed_and_waited = ref [ Ivar.read closed ] in
      let got_response = Ivar.create () in
      let payload = Bigstring.of_string "hello" in
      let%bind (_ : Rpc.Pipe_rpc.Id.t) =
        Rpc.Pipe_rpc.Expert.dispatch_iter
          Test_helpers.pipe_rpc
          client
          payload
          ~f:(fun message ~pos ~len ->
            let d =
              let%map () = Ivar.read got_response in
              print_s
                [%message
                  "Got buffer"
                    ~buf:(Bigstring.sub message ~pos ~len : Bigstring.t)
                    (pos : int)
                    (len : int)];
              let pos_ref = ref pos in
              let parsed = String.bin_read_t message ~pos_ref in
              [%test_result: int] !pos_ref ~expect:(pos + len);
              print_endline parsed
            in
            closed_and_waited := d :: !closed_and_waited;
            Wait d)
          ~closed:(fun kind ->
            Ivar.fill_exn closed ();
            match kind with
            | `By_remote_side -> ()
            | `Error error -> Error.raise error)
        >>| Or_error.join
        >>| Or_error.ok_exn
      in
      Ivar.fill_exn got_response ();
      print_endline "dispatch resolved";
      Deferred.all_unit !closed_and_waited)
  in
  [%expect
    {|
    dispatch resolved
    ("Got buffer" (buf "\bresponse") (pos 58) (len 9))
    response
    ("Got buffer" (buf "\bresponse") (pos 32) (len 9))
    response
    |}]
;;

let%expect_test "test State_rpc.dispatch_fold" =
  let%map () =
    Test_helpers.with_rpc_server_connection
      ~server_header:Test_helpers.Header.v2
      ~client_header:Test_helpers.Header.v2
      ~f:(fun ~client ~server:_ ~s_to_c:_ ~c_to_s:_ ->
      let closed = Ivar.create () in
      let closed_and_waited = ref [ Ivar.read closed ] in
      let got_response = Ivar.create () in
      let payload = Bigstring.of_string "hello" in
      let%bind (_ : Rpc.State_rpc.Id.t), final_result =
        Rpc.State_rpc.dispatch_fold
          Test_helpers.state_rpc
          client
          payload
          ~init:(fun state ->
            print_s [%sexp (state : Bigstring.t)];
            0)
          ~f:(fun acc message ->
            print_s [%message (acc : int) (message : Bigstring.t)];
            let d = Ivar.read got_response in
            closed_and_waited := d :: !closed_and_waited;
            acc + 1, Wait d)
          ~closed:(fun acc kind ->
            print_s
              [%message "closed" (acc : int) (kind : [ `By_remote_side | `Error of _ ])];
            Ivar.fill_exn closed ())
        >>| Or_error.join
        >>| Or_error.ok_exn
      in
      print_endline "dispatch resolved";
      Ivar.fill_exn got_response ();
      let%bind () = final_result in
      Deferred.all_unit !closed_and_waited)
  in
  [%expect
    {|
    state
    ((acc 0) (message update))
    ((acc 1) (message update))
    (closed (acc 2) (kind By_remote_side))
    dispatch resolved
    |}]
;;
