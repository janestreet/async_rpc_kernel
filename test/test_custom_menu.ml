open! Core
open! Async
module Rpc = Async_rpc.Rpc
module Menu = Async_rpc_kernel.Menu

let print_menu ~custom_menu =
  let%bind server =
    Rpc.Connection.serve
      ~custom_menu
      ~implementations:
        (Rpc.Implementations.create_exn
           ~implementations:[]
           ~on_unknown_rpc:`Raise
           ~on_exception:Log_on_background_exn)
      ~initial_connection_state:(fun (_ : Socket.Address.Inet.t) (_ : Rpc.Connection.t) ->
        ())
      ~where_to_listen:Tcp.Where_to_listen.of_port_chosen_by_os
      ()
  in
  let port = Tcp.Server.listening_on server in
  let where_to_connect =
    Tcp.Where_to_connect.of_host_and_port { Host_and_port.host = "localhost"; port }
  in
  let%bind client = Rpc.Connection.client where_to_connect >>| Result.ok_exn in
  match Rpc.Connection.peer_menu client with
  | None -> raise_s [%message "No menu received"]
  | Some menu ->
    let descriptions =
      Menu.supported_rpcs menu |> List.sort ~compare:[%compare: Rpc.Description.t]
    in
    print_s [%message (descriptions : Rpc.Description.t list)];
    let%bind () = Rpc.Connection.close client in
    Tcp.Server.close server
;;

let%expect_test "custom_menu overrides auto-generated menu" =
  let custom_menu =
    (* Fake rpcs which don't actually exist. *)
    Menu.of_supported_rpcs
      [ { Rpc.Description.name = "custom-rpc"; version = 1 }
      ; { Rpc.Description.name = "custom-rpc"; version = 2 }
      ]
      ~rpc_shapes:`Unknown
  in
  let%bind () = print_menu ~custom_menu:(fun () -> custom_menu) in
  [%expect
    {|
    (descriptions
     (((name custom-rpc) (version 1)) ((name custom-rpc) (version 2))))
    |}];
  return ()
;;

let%expect_test "custom_menu reflects dynamic state across connections" =
  let dynamic_menu =
    let menu_version = ref 0 in
    fun () ->
      incr menu_version;
      Menu.of_supported_rpcs
        [ { Rpc.Description.name = "dynamic-rpc"; version = !menu_version } ]
        ~rpc_shapes:`Unknown
  in
  let%bind () = print_menu ~custom_menu:dynamic_menu in
  [%expect {| (descriptions (((name dynamic-rpc) (version 1)))) |}];
  let%map () = print_menu ~custom_menu:dynamic_menu in
  [%expect {| (descriptions (((name dynamic-rpc) (version 2)))) |}]
;;
