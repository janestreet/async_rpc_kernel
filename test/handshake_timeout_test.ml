open! Core
open! Async

let test ~client_handshake_timeout_s ~server_handshake_timeout_s =
  let transport1, transport2 =
    let create_pipe tag =
      let r, w' = Pipe.create () in
      let r', w = Pipe.create () in
      let shapes =
        ref
          [ [%bin_shape:
              Async_rpc_kernel.Async_rpc_kernel_private.Connection.For_testing.Header.t
              Binio_printer_helper.With_length64.t]
          ; [%bin_shape:
              Nothing.t Binio_printer_helper.With_length.t
              Async_rpc_kernel.Async_rpc_kernel_private.Protocol.Message
              .maybe_needs_length
              Binio_printer_helper.With_length64.t]
          ]
      in
      Pipe.transfer' r' w' ~f:(fun bigstring_q ->
        (* This yield allows timeouts to fire, which is needed to cause the connections to
           be closed before negotiation is done *)
        let%map () = Scheduler.yield () in
        Queue.iter bigstring_q ~f:(fun bigstring ->
          match !shapes with
          | [] -> ()
          | shape :: rest ->
            shapes := rest;
            print_endline tag;
            Binio_printer_helper.parse_and_print shape bigstring ~pos:0);
        bigstring_q)
      |> don't_wait_for;
      r, w
    in
    let r1, w1 = create_pipe "c_to_s" in
    let r2, w2 = create_pipe "s_to_c" in
    let bigstring = Async_rpc_kernel.Pipe_transport.Kind.bigstring in
    ( Async_rpc_kernel.Pipe_transport.create bigstring r1 w2
    , Async_rpc_kernel.Pipe_transport.create bigstring r2 w1 )
  in
  let sec = Time_ns.Span.of_int_sec in
  let server =
    Async_rpc_kernel.Rpc.Connection.create
      ~handshake_timeout:(sec server_handshake_timeout_s)
      ~connection_state:(fun (_ : Rpc.Connection.t) -> ())
      transport1
  in
  let client =
    Async_rpc_kernel.Rpc.Connection.create
      ~handshake_timeout:(sec client_handshake_timeout_s)
      ~connection_state:(fun (_ : Rpc.Connection.t) -> ())
      transport2
  in
  let%bind client_conn = client
  and server_conn = server in
  print_s [%message (client_conn : (_, exn) Result.t) (server_conn : (_, exn) Result.t)];
  let%bind () =
    match client_conn with
    | Error _ -> return ()
    | Ok conn ->
      let%map client_menu_from_server = Rpc.Connection.peer_menu conn in
      print_s [%message (client_menu_from_server : _ option Or_error.t)]
  in
  let%bind () =
    match server_conn with
    | Error _ -> return ()
    | Ok conn ->
      let%map server_menu_from_client = Rpc.Connection.peer_menu conn in
      print_s [%message (server_menu_from_client : _ option Or_error.t)]
  in
  return ()
;;

let%expect_test "client 60s, server 60s" =
  let%bind () = test ~client_handshake_timeout_s:60 ~server_handshake_timeout_s:60 in
  [%expect
    {|
    c_to_s
    0900 0000 0000 0000    length= 9 (64-bit LE)
    04                       body= List: 4 items
    fd52 5043 00                   0: 4411474 (int)
    01                             1: 1 (int)
    02                             2: 2 (int)
    03                             3: 3 (int)
    s_to_c
    0900 0000 0000 0000    length= 9 (64-bit LE)
    04                       body= List: 4 items
    fd52 5043 00                   0: 4411474 (int)
    01                             1: 1 (int)
    02                             2: 2 (int)
    03                             3: 3 (int)
    ((client_conn (Ok _)) (server_conn (Ok _)))
    c_to_s
    0400 0000 0000 0000    length= 4 (64-bit LE)
    04                       body= Metadata
    00                             identification= None
    01                                       menu= Some
    00                                              List: 0 items
    s_to_c
    0400 0000 0000 0000    length= 4 (64-bit LE)
    04                       body= Metadata
    00                             identification= None
    01                                       menu= Some
    00                                              List: 0 items
    (client_menu_from_server (Ok (_)))
    (server_menu_from_client (Ok (_)))
    |}];
  return ()
;;

let%expect_test "client 0s, server 60s" =
  let%bind () = test ~client_handshake_timeout_s:0 ~server_handshake_timeout_s:60 in
  [%expect
    {|
    c_to_s
    0900 0000 0000 0000    length= 9 (64-bit LE)
    04                       body= List: 4 items
    fd52 5043 00                   0: 4411474 (int)
    01                             1: 1 (int)
    02                             2: 2 (int)
    03                             3: 3 (int)
    s_to_c
    0900 0000 0000 0000    length= 9 (64-bit LE)
    04                       body= List: 4 items
    fd52 5043 00                   0: 4411474 (int)
    01                             1: 1 (int)
    02                             2: 2 (int)
    03                             3: 3 (int)
    ((client_conn
      (Error
       (connection.ml.Handshake_error.Handshake_error
        (Timeout <created-directly>))))
     (server_conn (Ok _)))
    (server_menu_from_client
     (Error
      ("Connection closed before we could get peer metadata"
       (trying_to_get peer_menu) (connection_description <created-directly>)
       (close_reason ("RPC transport stopped")))))
    |}];
  return ()
;;

let%expect_test "client 60s, server 0s" =
  let%bind () = test ~client_handshake_timeout_s:60 ~server_handshake_timeout_s:0 in
  [%expect
    {|
    c_to_s
    0900 0000 0000 0000    length= 9 (64-bit LE)
    04                       body= List: 4 items
    fd52 5043 00                   0: 4411474 (int)
    01                             1: 1 (int)
    02                             2: 2 (int)
    03                             3: 3 (int)
    s_to_c
    0900 0000 0000 0000    length= 9 (64-bit LE)
    04                       body= List: 4 items
    fd52 5043 00                   0: 4411474 (int)
    01                             1: 1 (int)
    02                             2: 2 (int)
    03                             3: 3 (int)
    ((client_conn (Ok _))
     (server_conn
      (Error
       (connection.ml.Handshake_error.Handshake_error
        (Timeout <created-directly>)))))
    (client_menu_from_server
     (Error
      ("Connection closed before we could get peer metadata"
       (trying_to_get peer_menu) (connection_description <created-directly>)
       (close_reason ("RPC transport stopped")))))
    |}];
  return ()
;;

let%expect_test "client 0s, server 0s" =
  let%bind () = test ~client_handshake_timeout_s:0 ~server_handshake_timeout_s:0 in
  [%expect
    {|
    c_to_s
    0900 0000 0000 0000    length= 9 (64-bit LE)
    04                       body= List: 4 items
    fd52 5043 00                   0: 4411474 (int)
    01                             1: 1 (int)
    02                             2: 2 (int)
    03                             3: 3 (int)
    s_to_c
    0900 0000 0000 0000    length= 9 (64-bit LE)
    04                       body= List: 4 items
    fd52 5043 00                   0: 4411474 (int)
    01                             1: 1 (int)
    02                             2: 2 (int)
    03                             3: 3 (int)
    ((client_conn
      (Error
       (connection.ml.Handshake_error.Handshake_error
        (Timeout <created-directly>))))
     (server_conn
      (Error
       (connection.ml.Handshake_error.Handshake_error
        (Timeout <created-directly>)))))
    |}];
  return ()
;;
