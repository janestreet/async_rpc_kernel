open! Core
open! Async

let rpc =
  Rpc.Rpc.create
    ~name:"test_rpc"
    ~version:1
    ~bin_query:bin_unit
    ~bin_response:String.Stable.V1.bin_t
;;

let implementations =
  let implementation =
    Rpc.Rpc.implement rpc (fun () () ->
      match Async_rpc_kernel.Rpc_metadata.get () with
      | None -> return "no request id"
      | Some rqid -> return [%string "request %{rqid}"])
  in
  Rpc.Implementations.create_exn
    ~implementations:[ implementation ]
    ~on_unknown_rpc:`Raise
;;

let with_rpc_server_connection ~f =
  let%bind server =
    Rpc.Connection.serve
      ~implementations
      ~initial_connection_state:(fun _ _ -> ())
      ~where_to_listen:Tcp.Where_to_listen.of_port_chosen_by_os
      ()
  in
  let port = Tcp.Server.listening_on server in
  let where_to_connect =
    Tcp.Where_to_connect.of_host_and_port { Host_and_port.host = "localhost"; port }
  in
  let%bind conn = Rpc.Connection.client where_to_connect >>| Result.ok_exn in
  let%bind result = f conn in
  let%bind () = Rpc.Connection.close conn
  and () = Tcp.Server.close server in
  return result
;;

let%expect_test "no metadata" =
  let%bind response =
    with_rpc_server_connection ~f:(fun conn -> Rpc.Rpc.dispatch rpc conn ())
  in
  [%sexp_of: string Or_error.t] response |> print_s;
  return [%expect {| (Ok "no request id") |}]
;;

let%expect_test "with metadata" =
  let%bind response =
    with_rpc_server_connection ~f:(fun conn ->
      Rpc.Rpc.dispatch ~metadata:"0001" rpc conn ())
  in
  [%sexp_of: string Or_error.t] response |> print_s;
  return [%expect {| (Ok "request 0001") |}]
;;
