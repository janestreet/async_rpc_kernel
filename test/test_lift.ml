open! Core
open! Async

let test_lift_on_all_rpc_dispatch
  ?(header = Test_helpers.Header.v1)
  ?expect_exception
  ~lift
  ~return_state
  ()
  =
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
    match expect_exception, response with
    | None, Error (error : Async_rpc_kernel.Rpc_error.t) ->
      if Result.is_error response
      then
        raise_s
          [%message "Expected Ok but got error" (error : Async_rpc_kernel.Rpc_error.t)]
    | Some expect_exception, Error rpc_error -> assert (expect_exception rpc_error)
    | Some _, Ok _ -> raise_s [%message "Expected error but got Ok"]
    | None, Ok _ -> ()
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
    ~header
    ~f:(fun conn (_ : Test_helpers.Tap.t) ->
      let%bind () = dispatch ~conn Rpc.Rpc.dispatch' Test_helpers.rpc in
      let%bind () =
        (* Skip one-way RPC because exceptions close the connection. *)
        match expect_exception with
        | Some _ -> return ()
        | None ->
          let%bind () = dispatch_one_way ~conn in
          return ()
      in
      let%bind () = dispatch ~conn Rpc.Pipe_rpc.dispatch' Test_helpers.pipe_rpc in
      let%bind () = dispatch ~conn Rpc.State_rpc.dispatch' Test_helpers.state_rpc in
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

let expect_sexp_with_string s sexp =
  Sexp.to_string sexp |> String.is_substring ~substring:s
;;

let expect_exn_with_string s = function
  | Async_rpc_kernel.Rpc_error.Uncaught_exn sexp -> expect_sexp_with_string s sexp
  | (_ : Async_rpc_kernel.Rpc_error.t) -> false
;;

let%expect_test "Exception in [lift] function returns error on [dispatch]." =
  test_lift_on_all_rpc_dispatch
    ~expect_exception:(expect_exn_with_string "Exception!")
    ~lift:Rpc.Implementation.lift
    ~return_state:(fun () -> failwith "Exception!")
    ()
;;

let%expect_test "Exception in [lift_deferred] returns error on [dispatch]." =
  test_lift_on_all_rpc_dispatch
    ~expect_exception:(expect_exn_with_string "Exception!")
    ~lift:Rpc.Implementation.lift_deferred
    ~return_state:(fun () -> failwith "Exception!")
    ()
;;

let%expect_test "deferred exception in [lift_deferred] returns error on [dispatch]." =
  test_lift_on_all_rpc_dispatch
    ~expect_exception:(expect_exn_with_string "Exception!")
    ~lift:Rpc.Implementation.lift_deferred
    ~return_state:(fun () ->
      let%bind () = Scheduler.yield () in
      failwith "Exception!")
    ()
;;

let%expect_test "authorization runs on every rpc" =
  test_lift_on_all_rpc_dispatch
    ~lift:Rpc.Implementation.with_authorization
    ~return_state:(fun () -> Async_rpc_kernel.Or_not_authorized.Authorized ())
    ()
;;

let%expect_test "deferred authorization runs on every rpc" =
  test_lift_on_all_rpc_dispatch
    ~lift:Rpc.Implementation.with_authorization_deferred
    ~return_state:(fun () ->
      let%map () = Scheduler.yield () in
      Async_rpc_kernel.Or_not_authorized.Authorized ())
    ()
;;

let%expect_test "authorization rejection leads to rpc failures" =
  test_lift_on_all_rpc_dispatch
    ~header:Test_helpers.Header.v3
    ~expect_exception:(function
      | Async_rpc_kernel.Rpc_error.Authorization_failure s ->
        expect_sexp_with_string "bad auth" s
      | (_ : Async_rpc_kernel.Rpc_error.t) -> false)
    ~lift:Rpc.Implementation.with_authorization
    ~return_state:(fun () ->
      Async_rpc_kernel.Or_not_authorized.Not_authorized (Error.of_string "bad auth"))
    ()
;;

let%expect_test "deferred authorization rejection leads to rpc failures" =
  test_lift_on_all_rpc_dispatch
    ~header:Test_helpers.Header.v3
    ~expect_exception:(function
      | Async_rpc_kernel.Rpc_error.Authorization_failure s ->
        expect_sexp_with_string "bad auth" s
      | (_ : Async_rpc_kernel.Rpc_error.t) -> false)
    ~lift:Rpc.Implementation.with_authorization_deferred
    ~return_state:(fun () ->
      let%map () = Scheduler.yield () in
      Async_rpc_kernel.Or_not_authorized.Not_authorized (Error.of_string "bad auth"))
    ()
;;

let%expect_test "lift error" =
  let test_lift_on_all_rpc_dispatch =
    test_lift_on_all_rpc_dispatch
      ~lift:Rpc.Implementation.try_lift
      ~return_state:(fun () -> Error (Error.of_string "couldn't lift"))
  in
  let%bind () =
    test_lift_on_all_rpc_dispatch
      ~header:Test_helpers.Header.v5
      ~expect_exception:(function
        | Async_rpc_kernel.Rpc_error.Lift_error s ->
          expect_sexp_with_string "couldn't lift" s
        | (_ : Async_rpc_kernel.Rpc_error.t) -> false)
      ()
  in
  let%bind () =
    test_lift_on_all_rpc_dispatch
      ~header:Test_helpers.Header.v4
      ~expect_exception:(function
        | Async_rpc_kernel.Rpc_error.Unknown s ->
          expect_sexp_with_string "couldn't lift" s
        | (_ : Async_rpc_kernel.Rpc_error.t) -> false)
      ()
  in
  test_lift_on_all_rpc_dispatch
    ~header:Test_helpers.Header.v2
    ~expect_exception:(function
      | Async_rpc_kernel.Rpc_error.Uncaught_exn s ->
        expect_sexp_with_string "couldn't lift" s
      | (_ : Async_rpc_kernel.Rpc_error.t) -> false)
    ()
;;
