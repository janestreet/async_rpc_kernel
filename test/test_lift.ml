open! Core
open! Async

let test_lift_on_all_rpc_dispatch ?expect_exception ~lift ~return_state () =
  let reader, writer = Pipe.create () in
  let expect_lift_called ~f =
    let%bind result = f () in
    let%bind () =
      match%bind Pipe.read reader with
      | `Ok () -> return ()
      | `Eof -> failwith "Did not see a call to lift"
    in
    return result
  in
  let maybe_expect_error response =
    Option.iter expect_exception ~f:(fun contains ->
      let string = [%sexp_of: _ Or_error.t] response |> Sexp.to_string in
      assert (Result.is_error response);
      assert (String.is_substring string ~substring:contains))
  in
  let payload = Bigstring.of_string "test payload" in
  let dispatch ~conn dispatch rpc =
    let%bind response = expect_lift_called ~f:(fun () -> dispatch rpc conn payload) in
    maybe_expect_error response;
    return ()
  in
  let dispatch_one_way ~conn =
    let%bind _response =
      expect_lift_called ~f:(fun () ->
        Rpc.One_way.dispatch Test_helpers.one_way_rpc conn payload |> return)
    in
    return ()
  in
  Test_helpers.with_circular_connection
    ~lift_implementation:(fun implementation ->
      lift implementation ~f:(fun () ->
        Pipe.write_without_pushback writer ();
        return_state ()))
    ~header:Test_helpers.Header.v1
    ~f:(fun conn (_ : Test_helpers.Tap.t) ->
      let%bind () = dispatch ~conn Rpc.Rpc.dispatch Test_helpers.rpc in
      let%bind () =
        (* Skip one-way RPC because exceptions close the connection. *)
        match expect_exception with
        | Some _ -> return ()
        | None ->
          let%bind () = dispatch_one_way ~conn in
          return ()
      in
      let%bind () = dispatch ~conn Rpc.Pipe_rpc.dispatch Test_helpers.pipe_rpc in
      let%bind () = dispatch ~conn Rpc.State_rpc.dispatch Test_helpers.state_rpc in
      return ())
    ()
;;

let%expect_test "[lift] functions are called on every [dispatch]." =
  test_lift_on_all_rpc_dispatch ~lift:Rpc.Implementation.lift ~return_state:Fn.id ()
;;

let%expect_test "[lift_deferred] functions are called on every [dispatch]." =
  test_lift_on_all_rpc_dispatch
    ~lift:Rpc.Implementation.lift_deferred
    ~return_state:return
    ()
;;

let%expect_test "Exception in [lift] function returns error on [dispatch]." =
  test_lift_on_all_rpc_dispatch
    ~expect_exception:"Exception!"
    ~lift:Rpc.Implementation.lift
    ~return_state:(fun () -> failwith "Exception!")
    ()
;;

let%expect_test "Exception in [lift_deferred] returns error on [dispatch]." =
  test_lift_on_all_rpc_dispatch
    ~expect_exception:"Exception!"
    ~lift:Rpc.Implementation.lift_deferred
    ~return_state:(fun () -> failwith "Exception!")
    ()
;;
