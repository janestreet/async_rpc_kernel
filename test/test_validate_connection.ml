open! Core
open! Async

let rpc =
  Rpc.Rpc.create
    ~name:"test_rpc"
    ~version:1
    ~bin_query:bin_unit
    ~bin_response:bin_unit
    ~include_in_error_count:Only_on_exn
;;

let implementations =
  let implementation = Rpc.Rpc.implement rpc (fun () () -> return ()) in
  Rpc.Implementations.create_exn
    ~implementations:[ implementation ]
    ~on_unknown_rpc:`Raise
    ~on_exception:Log_on_background_exn
;;

let default_validate_connection ~expected_identity ~identification_from_peer =
  let%map () = Deferred.unit in
  match identification_from_peer with
  | None -> Rpc.Or_not_authorized.Not_authorized (Error.of_string "Missing peer identity")
  | Some identity ->
    if Bigstring.equal identity expected_identity
    then Rpc.Or_not_authorized.Authorized ()
    else
      Rpc.Or_not_authorized.Not_authorized
        (Error.of_string [%string "Wrong peer identity: %{identity#Bigstring}"])
;;

let with_rpc_server_connection
  ?(implementations = implementations)
  ?server_identification
  ?client_identification
  f
  ~on_client_connected
  =
  let%bind server =
    Rpc.Connection.serve
      ?identification:server_identification
      ~validate_connection:
        (default_validate_connection
           ~expected_identity:Test_helpers.client_identification)
      ~implementations
      ~initial_connection_state:(fun _ conn -> on_client_connected conn)
      ~where_to_listen:Tcp.Where_to_listen.of_port_chosen_by_os
      ()
  in
  let port = Tcp.Server.listening_on server in
  let where_to_connect =
    Tcp.Where_to_connect.of_host_and_port { Host_and_port.host = "localhost"; port }
  in
  let%bind.Deferred.Or_error conn =
    Rpc.Connection.client
      ?identification:client_identification
      ~validate_connection:
        (default_validate_connection
           ~expected_identity:Test_helpers.server_identification)
      where_to_connect
    >>| Or_error.of_exn_result
  in
  let%bind result = f conn in
  let%bind () = Rpc.Connection.close conn
  and () = Tcp.Server.close server in
  return result
;;

let%expect_test "connection authorized" =
  let%bind response =
    with_rpc_server_connection
      ~client_identification:Test_helpers.client_identification
      ~server_identification:Test_helpers.server_identification
      (fun conn -> Rpc.Rpc.dispatch rpc conn ())
      ~on_client_connected:ignore
  in
  [%sexp_of: unit Or_error.t] response |> print_s;
  return [%expect {| (Ok ()) |}]
;;

let%expect_test "no server identification" =
  let%bind response =
    with_rpc_server_connection
      ~client_identification:Test_helpers.client_identification
      (fun (_ : Rpc.Connection.t) -> failwith "connection should not be established")
      ~on_client_connected:(fun _ -> print_endline "client connected")
  in
  [%sexp_of: unit Or_error.t] response |> print_s;
  return
    [%expect
      {|
      client connected
      (Error
       (handshake_error.ml.Handshake_error
        ((Connection_validation_failed "Missing peer identity")
         ("Client connected via TCP" (localhost PORT)))))
      |}]
;;

let%expect_test "no client identification" =
  let%bind response =
    with_rpc_server_connection
      ~server_identification:Test_helpers.server_identification
      (fun conn ->
        let%bind response = Rpc.Rpc.dispatch rpc conn () in
        let%map close_reason =
          Rpc.Connection.close_reason_structured conn ~on_close:`started
        in
        match close_reason.reason.kind with
        | Connection_validation_failed -> response
        | _ ->
          raise_s
            [%message
              "Unexpected close reason kind"
                (close_reason : Async_rpc_kernel.Close_reason.t)])
      ~on_client_connected:(fun _ -> print_endline "client connected")
  in
  [%sexp_of: unit Or_error.t] response |> print_s;
  return
    [%expect
      {|
      (Error
       ((rpc_error
         (Connection_closed
          ((("Connection closed by remote side:"
             (Connection_validation_failed "Missing peer identity"))
            (connection_description ("Client connected via TCP" (localhost PORT)))))))
        (connection_description ("Client connected via TCP" (localhost PORT)))
        (rpc_name test_rpc) (rpc_version 1)))
      |}]
;;

let%expect_test "wrong client identification" =
  let%bind response =
    with_rpc_server_connection
      ~client_identification:(Bigstring.of_string "identity")
      ~server_identification:Test_helpers.server_identification
      (fun conn ->
        let%bind response = Rpc.Rpc.dispatch rpc conn () in
        let%map close_reason =
          Rpc.Connection.close_reason_structured conn ~on_close:`started
        in
        match close_reason.reason.kind with
        | Connection_validation_failed -> response
        | _ ->
          raise_s
            [%message
              "Unexpected close reason kind"
                (close_reason : Async_rpc_kernel.Close_reason.t)])
      ~on_client_connected:(fun _ -> print_endline "client connected")
  in
  [%sexp_of: unit Or_error.t] response |> print_s;
  return
    [%expect
      {|
      (Error
       ((rpc_error
         (Connection_closed
          ((("Connection closed by remote side:"
             (Connection_validation_failed "Wrong peer identity: identity"))
            (connection_description ("Client connected via TCP" (localhost PORT)))))))
        (connection_description ("Client connected via TCP" (localhost PORT)))
        (rpc_name test_rpc) (rpc_version 1)))
      |}]
;;

let%expect_test "wrong server identification" =
  let%bind response =
    with_rpc_server_connection
      ~client_identification:Test_helpers.client_identification
      ~server_identification:(Bigstring.of_string "identity")
      (fun (_ : Rpc.Connection.t) -> failwith "connection should not be established")
      ~on_client_connected:(fun _ -> print_endline "client connected")
  in
  [%sexp_of: unit Or_error.t] response |> print_s;
  return
    [%expect
      {|
      client connected
      (Error
       (handshake_error.ml.Handshake_error
        ((Connection_validation_failed "Wrong peer identity: identity")
         ("Client connected via TCP" (localhost PORT)))))
      |}]
;;
