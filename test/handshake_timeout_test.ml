open! Core
open! Async

let test ~client_handshake_timeout_s ~server_handshake_timeout_s =
  Dynamic.set_root Backtrace.elide true;
  let transport1, transport2, finished_printing =
    let create_pipe tag =
      let pipe_ivar1 = Ivar.create () in
      let pipe_ivar2 = Ivar.create () in
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
      let transfer_and_print_messages reader writer pipe_ivar =
        don't_wait_for
          (let%bind () =
             Pipe.transfer' reader writer ~f:(fun bigstring_q ->
               (* This yield allows timeouts to fire, which is needed to cause the
                  connections to be closed before negotiation is done *)
               let%map () = Scheduler.yield () in
               Queue.iter bigstring_q ~f:(fun bigstring ->
                 match !shapes with
                 | [] -> ()
                 | shape :: rest ->
                   shapes := rest;
                   print_endline tag;
                   Binio_printer_helper.parse_and_print [ shape ] bigstring ~pos:0);
               bigstring_q)
           in
           Ivar.fill_exn pipe_ivar ();
           return ())
      in
      transfer_and_print_messages r' w' pipe_ivar1;
      transfer_and_print_messages r w pipe_ivar2;
      let finished_printing =
        let%map () = Ivar.read pipe_ivar1
        and () = Ivar.read pipe_ivar2 in
        ()
      in
      r, w, finished_printing
    in
    let r1, w1, finished_printing1 = create_pipe "c_to_s" in
    let r2, w2, finished_printing2 = create_pipe "s_to_c" in
    let bigstring = Async_rpc_kernel.Pipe_transport.Kind.bigstring in
    let finished_printing =
      let%map () = finished_printing1
      and () = finished_printing2 in
      ()
    in
    ( Async_rpc_kernel.Pipe_transport.create bigstring r1 w2
    , Async_rpc_kernel.Pipe_transport.create bigstring r2 w1
    , finished_printing )
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
      let%bind client_menu_from_server = Rpc.Connection.peer_menu conn in
      let%map () = Rpc.Connection.close conn in
      print_s [%message (client_menu_from_server : _ option Or_error.t)]
  in
  let%bind () =
    match server_conn with
    | Error _ -> return ()
    | Ok conn ->
      let%bind server_menu_from_client = Rpc.Connection.peer_menu conn in
      let%map () = Rpc.Connection.close conn in
      print_s [%message (server_menu_from_client : _ option Or_error.t)]
  in
  let%bind () = finished_printing in
  return ()
;;

let%expect_test "client 60s, server 60s" =
  let%bind () = test ~client_handshake_timeout_s:60 ~server_handshake_timeout_s:60 in
  [%expect
    {|
    c_to_s
    1100 0000 0000 0000    length= 17 (64-bit LE)
    0c                       body= List: 12 items
    fd52 5043 00                    0: 4411474 (int)
    01                              1: 1 (int)
    02                              2: 2 (int)
    03                              3: 3 (int)
    04                              4: 4 (int)
    05                              5: 5 (int)
    06                              6: 6 (int)
    07                              7: 7 (int)
    08                              8: 8 (int)
    09                              9: 9 (int)
    0a                             10: 10 (int)
    0b                             11: 11 (int)
    s_to_c
    1100 0000 0000 0000    length= 17 (64-bit LE)
    0c                       body= List: 12 items
    fd52 5043 00                    0: 4411474 (int)
    01                              1: 1 (int)
    02                              2: 2 (int)
    03                              3: 3 (int)
    04                              4: 4 (int)
    05                              5: 5 (int)
    06                              6: 6 (int)
    07                              7: 7 (int)
    08                              8: 8 (int)
    09                              9: 9 (int)
    0a                             10: 10 (int)
    0b                             11: 11 (int)
    c_to_s
    0500 0000 0000 0000    length= 5 (64-bit LE)
    07                       body= Metadata_v2
    00                             identification= None
    01                                       menu= Some
    00                                              descriptions= Array: 0 items
    00                                                   digests= None
    s_to_c
    0500 0000 0000 0000    length= 5 (64-bit LE)
    07                       body= Metadata_v2
    00                             identification= None
    01                                       menu= Some
    00                                              descriptions= Array: 0 items
    00                                                   digests= None
    ((client_conn (Ok _)) (server_conn (Ok _)))
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
    1100 0000 0000 0000    length= 17 (64-bit LE)
    0c                       body= List: 12 items
    fd52 5043 00                    0: 4411474 (int)
    01                              1: 1 (int)
    02                              2: 2 (int)
    03                              3: 3 (int)
    04                              4: 4 (int)
    05                              5: 5 (int)
    06                              6: 6 (int)
    07                              7: 7 (int)
    08                              8: 8 (int)
    09                              9: 9 (int)
    0a                             10: 10 (int)
    0b                             11: 11 (int)
    s_to_c
    1100 0000 0000 0000    length= 17 (64-bit LE)
    0c                       body= List: 12 items
    fd52 5043 00                    0: 4411474 (int)
    01                              1: 1 (int)
    02                              2: 2 (int)
    03                              3: 3 (int)
    04                              4: 4 (int)
    05                              5: 5 (int)
    06                              6: 6 (int)
    07                              7: 7 (int)
    08                              8: 8 (int)
    09                              9: 9 (int)
    0a                             10: 10 (int)
    0b                             11: 11 (int)
    ((client_conn
      (Error (handshake_error.ml.Handshake_error (Timeout <created-directly>))))
     (server_conn
      (Error
       (handshake_error.ml.Handshake_error
        ((Transport_closed_during_step Connection_metadata) <created-directly>)))))
    |}];
  return ()
;;

let%expect_test "client 60s, server 0s" =
  let%bind () = test ~client_handshake_timeout_s:60 ~server_handshake_timeout_s:0 in
  [%expect
    {|
    c_to_s
    1100 0000 0000 0000    length= 17 (64-bit LE)
    0c                       body= List: 12 items
    fd52 5043 00                    0: 4411474 (int)
    01                              1: 1 (int)
    02                              2: 2 (int)
    03                              3: 3 (int)
    04                              4: 4 (int)
    05                              5: 5 (int)
    06                              6: 6 (int)
    07                              7: 7 (int)
    08                              8: 8 (int)
    09                              9: 9 (int)
    0a                             10: 10 (int)
    0b                             11: 11 (int)
    s_to_c
    1100 0000 0000 0000    length= 17 (64-bit LE)
    0c                       body= List: 12 items
    fd52 5043 00                    0: 4411474 (int)
    01                              1: 1 (int)
    02                              2: 2 (int)
    03                              3: 3 (int)
    04                              4: 4 (int)
    05                              5: 5 (int)
    06                              6: 6 (int)
    07                              7: 7 (int)
    08                              8: 8 (int)
    09                              9: 9 (int)
    0a                             10: 10 (int)
    0b                             11: 11 (int)
    ((client_conn
      (Error
       (handshake_error.ml.Handshake_error
        ((Transport_closed_during_step Connection_metadata) <created-directly>))))
     (server_conn
      (Error (handshake_error.ml.Handshake_error (Timeout <created-directly>)))))
    |}];
  return ()
;;

let%expect_test "client 0s, server 0s" =
  let%bind () = test ~client_handshake_timeout_s:0 ~server_handshake_timeout_s:0 in
  [%expect
    {|
    c_to_s
    1100 0000 0000 0000    length= 17 (64-bit LE)
    0c                       body= List: 12 items
    fd52 5043 00                    0: 4411474 (int)
    01                              1: 1 (int)
    02                              2: 2 (int)
    03                              3: 3 (int)
    04                              4: 4 (int)
    05                              5: 5 (int)
    06                              6: 6 (int)
    07                              7: 7 (int)
    08                              8: 8 (int)
    09                              9: 9 (int)
    0a                             10: 10 (int)
    0b                             11: 11 (int)
    s_to_c
    1100 0000 0000 0000    length= 17 (64-bit LE)
    0c                       body= List: 12 items
    fd52 5043 00                    0: 4411474 (int)
    01                              1: 1 (int)
    02                              2: 2 (int)
    03                              3: 3 (int)
    04                              4: 4 (int)
    05                              5: 5 (int)
    06                              6: 6 (int)
    07                              7: 7 (int)
    08                              8: 8 (int)
    09                              9: 9 (int)
    0a                             10: 10 (int)
    0b                             11: 11 (int)
    ((client_conn
      (Error (handshake_error.ml.Handshake_error (Timeout <created-directly>))))
     (server_conn
      (Error (handshake_error.ml.Handshake_error (Timeout <created-directly>)))))
    |}];
  [%expect {| |}];
  return ()
;;
