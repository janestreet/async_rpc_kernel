open! Core
open! Async
open! Import

module%test [@name "Proxy Server"] _ = struct
  let real_rpc =
    Rpc.Rpc.create
      ~name:"real-rpc"
      ~version:1
      ~bin_query:bin_unit
      ~bin_response:bin_unit
      ~include_in_error_count:Only_on_exn
  ;;

  let real_pipe_rpc =
    Rpc.Pipe_rpc.create
      ~name:"real-pipe-rpc"
      ~version:1
      ~bin_query:bin_int
      ~bin_response:bin_int
      ~bin_error:Error.bin_t
      ()
  ;;

  let proxy_implementations =
    Rpc.Implementations.Expert.create_exn
      ~implementations:[]
      ~on_exception:(Raise_to_monitor Monitor.main)
      ~on_unknown_rpc:
        (`Expert
          (fun to_ ~rpc_tag ~version ~metadata:_ responder query ~pos ~len ->
            let%bind peer_identification = Rpc.Connection.peer_identification to_ in
            print_s
              [%message
                "Proxying rpc"
                  (peer_identification : Bigstring.t option)
                  (rpc_tag : string)
                  (version : int)];
            let res =
              Rpc.Rpc.Expert.schedule_dispatch
                to_
                ~rpc_tag
                ~version
                query
                ~pos
                ~len
                ~handle_response:(fun response ~pos ~len ->
                  let res =
                    Rpc.Rpc.Expert.Responder.schedule responder response ~pos ~len
                  in
                  match res with
                  | `Connection_closed -> return ()
                  | `Flushed f -> f)
                ~handle_error:(fun error ->
                  Rpc.Rpc.Expert.Responder.write_error responder error)
            in
            match res with
            | `Flushed f -> f
            | `Connection_closed -> return ()))
  ;;

  let real_implementations =
    Rpc.Implementations.create_exn
      ~implementations:
        [ Rpc.Rpc.implement real_rpc (fun () () -> return ())
        ; Rpc.Pipe_rpc.implement_direct
            real_pipe_rpc
            (fun () count direct_stream_writer ->
               don't_wait_for
                 (let%bind () =
                    Rpc.Pipe_rpc.Direct_stream_writer.started direct_stream_writer
                  in
                  Deferred.List.init count ~how:`Sequential ~f:(fun i ->
                    match
                      Rpc.Pipe_rpc.Direct_stream_writer.write direct_stream_writer i
                    with
                    | `Flushed flushed -> flushed
                    | `Closed -> Deferred.unit)
                  >>| (ignore : unit list -> unit));
               Deferred.Result.return ())
        ]
      ~on_exception:(Raise_to_monitor Monitor.main)
      ~on_unknown_rpc:`Raise
  ;;

  let with_proxied_rpc_connection ~f =
    let with_rpc_server_connection
      ~implementations
      ~server_identification
      ~initial_connection_state
      ~f
      =
      let%bind server =
        Async_rpc.Rpc.Connection.serve
          ~implementations
          ~initial_connection_state
          ~where_to_listen:Tcp.Where_to_listen.of_port_chosen_by_os
          ~identification:server_identification
          ()
      in
      let port = Tcp.Server.listening_on server in
      let where_to_connect =
        Tcp.Where_to_connect.of_host_and_port { Host_and_port.host = "localhost"; port }
      in
      let%bind conn =
        Async_rpc.Rpc.Connection.client where_to_connect >>| Result.ok_exn
      in
      let%bind result = f conn in
      let%bind () = Rpc.Connection.close conn
      and () = Tcp.Server.close server in
      return result
    in
    with_rpc_server_connection
      ~implementations:real_implementations
      ~initial_connection_state:(fun (_ : Socket.Address.Inet.t) (_ : Rpc.Connection.t) ->
        ())
      ~server_identification:(Bigstring.of_string "real-server")
      ~f:(fun real_connection ->
        with_rpc_server_connection
          ~implementations:proxy_implementations
          ~initial_connection_state:
            (fun
              (_ : Socket.Address.Inet.t) (_ : Rpc.Connection.t) -> real_connection)
          ~server_identification:(Bigstring.of_string "proxy-server")
          ~f)
  ;;

  let%expect_test "proxied rpc" =
    with_proxied_rpc_connection ~f:(fun proxy_connection ->
      let%map () = Rpc.Rpc.dispatch_exn real_rpc proxy_connection () in
      [%expect
        {|
        ("Proxying rpc" (peer_identification (real-server)) (rpc_tag real-rpc)
         (version 1))
        |}])
  ;;

  let%expect_test "proxied pipe-rpcs do NOT work" =
    let%map () =
      with_proxied_rpc_connection ~f:(fun proxy_connection ->
        Rpc.Pipe_rpc.dispatch_iter real_pipe_rpc proxy_connection 5 ~f:(function
          | Update i ->
            print_s [%message "Received update" (i : int)];
            Continue
          | Closed reason ->
            print_s
              [%message
                "Received close" (reason : [ `By_remote_side | `Error of Error.t ])];
            Continue)
        >>| ok_exn
        >>| Result.map ~f:(ignore : Rpc.Pipe_rpc.Id.t -> unit)
        >>| [%sexp_of: unit Or_error.t]
        >>| print_s)
    in
    (* The rpc intial message succeeds but we never receive an update since the current
       proxied rpc technique doesn't allow for multiple responses. *)
    [%expect
      {|
      ("Proxying rpc" (peer_identification (real-server)) (rpc_tag real-pipe-rpc)
       (version 1))
      (Ok ())
      ("Received close"
       (reason
        (Error
         (Connection_closed
          ((("Connection closed by local side:" Rpc.Connection.close)
            (connection_description ("Client connected via TCP" (localhost PORT)))))))))
      |}]
  ;;
end
