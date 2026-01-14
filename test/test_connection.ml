open! Core
open! Async
open! Import
module Rpc = Async_rpc.Rpc
module Payload = Test_helpers.Payload

let print_header = Test_helpers.Tap.print_header
let print_headers = Test_helpers.Tap.print_headers

let print_payload_messages tap =
  Test_helpers.Tap.print_messages tap [ [%bin_shape: Payload.t] ]
;;

let print_stream_messages tap =
  Test_helpers.Tap.print_messages
    tap
    [ [%bin_shape:
        Bigstring.Stable.V1.t Binio_printer_helper.With_length.t
          Protocol.Stream_query.needs_length]
    ; [%bin_shape: (unit, Error.Stable.V2.t) Protocol.Stream_initial_message.t]
    ; [%bin_shape:
        (Bigstring.Stable.V1.t, Error.Stable.V2.t) Protocol.Stream_initial_message.t]
    ; [%bin_shape:
        Bigstring.Stable.V1.t Binio_printer_helper.With_length.t
          Protocol.Stream_response_data.needs_length]
    ]
;;

let print_payload_messages_bidirectional =
  Test_helpers.Tap.print_messages_bidirectional [ [%bin_shape: Payload.t] ]
;;

let dispatch ~client =
  let payload = Payload.create () in
  let%map _response = Rpc.Rpc.dispatch_exn Test_helpers.sort_rpc client payload in
  ()
;;

let dispatch_expert ~client ~payload =
  let writer = [%bin_writer: int array] in
  let buf = Bin_prot.Utils.bin_dump writer payload in
  let wait_for_response = Ivar.create () in
  let handle_response _buf ~pos:_ ~len:_ =
    Ivar.fill_exn wait_for_response ();
    return ()
  in
  match
    Rpc.Rpc.Expert.dispatch
      client
      ~rpc_tag:(Rpc.Rpc.name Test_helpers.sort_rpc)
      ~version:(Rpc.Rpc.version Test_helpers.sort_rpc)
      buf
      ~pos:0
      ~len:(Bigstring.length buf)
      ~handle_response
      ~handle_error:(fun err -> failwiths "expert rpc error" err [%sexp_of: Error.t])
  with
  | `Connection_closed -> failwith "connection closed"
  | `Ok -> Ivar.read wait_for_response
;;

let connection_test_id ~client ~server ~s_to_c ~c_to_s =
  print_headers ~s_to_c ~c_to_s;
  let%bind server_id_from_client = Rpc.Connection.peer_identification client in
  let%map client_id_from_server = Rpc.Connection.peer_identification server in
  print_payload_messages_bidirectional ~s_to_c ~c_to_s;
  print_s
    [%message
      (server_id_from_client : Bigstring.t option)
        (client_id_from_server : Bigstring.t option)]
;;

let connection_test_menu ~client ~server ~s_to_c:_ ~c_to_s:_ =
  let%bind client_menu = Rpc.Connection.peer_menu server >>| ok_exn in
  let%map server_menu = Rpc.Connection.peer_menu client >>| ok_exn in
  match client_menu, server_menu with
  | None, None -> print_s [%message "No menus received"]
  | Some client_menu, Some server_menu ->
    print_s [%message (client_menu : Async_rpc_kernel.Menu.With_digests_in_sexp.t)];
    print_s [%message (server_menu : Async_rpc_kernel.Menu.With_digests_in_sexp.t)]
  | _ ->
    raise_s
      [%message
        "Only one of the client or server menu was received"
          (client_menu : Async_rpc_kernel.Menu.With_digests_in_sexp.t option)
          (server_menu : Async_rpc_kernel.Menu.With_digests_in_sexp.t option)]
;;

let connection_test ?provide_rpc_shapes () ~server_header ~client_header ~f
  : unit Deferred.t
  =
  Test_helpers.with_rpc_server_connection
    ?provide_rpc_shapes
    ()
    ~server_header
    ~client_header
    ~f:(fun ~client ~server ~s_to_c ~c_to_s -> f ~client ~server ~s_to_c ~c_to_s)
;;

let%expect_test "latest version simple RPC dispatches" =
  Test_helpers.with_circular_connection
    ~header:Test_helpers.Header.latest
    ~f:(fun conn tap ->
      print_header tap;
      [%expect
        {|
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

        5000 0000 0000 0000    length= 80 (64-bit LE)
        07                       body= Metadata_v2
        00                             identification= None
        01                                       menu= Some
        06                                              descriptions= Array: 6 items
        0473 6f72 74                                                  0:    name= sort (4 bytes)
        01                                                               version= 1 (int)
        1074 6573 742d 6f6e    ...
        652d 7761 792d 7270    ...
        63                                                            1:    name= test-on... (16 bytes)
        01                                                               version= 1 (int)
        0d74 6573 742d 7069    ...
        7065 2d72 7063                                                2:    name= test-pi... (13 bytes)
        01                                                               version= 1 (int)
        0874 6573 742d 7270    ...
        63                                                            3:    name= test-rpc (8 bytes)
        01                                                               version= 1 (int)
        0874 6573 742d 7270    ...
        63                                                            4:    name= test-rpc (8 bytes)
        02                                                               version= 2 (int)
        0e74 6573 742d 7374    ...
        6174 652d 7270 63                                             5:    name= test-st... (14 bytes)
        01                                                               version= 1 (int)
        00                                                   digests= None
        |}];
      let%bind _response =
        Rpc.Rpc.dispatch_exn Test_helpers.sort_rpc conn (Payload.create ())
      in
      print_payload_messages tap;
      [%expect
        {|
        1100 0000 0000 0000    length= 17 (64-bit LE)
        0c                       body= Query_v4
        01                             specifier= Rank
        00                                        0 (int)
        01                                    id= 1 (int)
        00                              metadata= None
        0b                                  data= length= 11 (int)
        04                                          body= Array: 4 items
        fe80 01                                           0: 384 (int)
        fee5 03                                           1: 997 (int)
        fea6 00                                           2: 166 (int)
        23                                                3: 35 (int)

        0100 0000 0000 0000    length= 1 (64-bit LE)
        00                       body= Heartbeat

        1100 0000 0000 0000    length= 17 (64-bit LE)
        08                       body= Response_v2
        01                                          id= 1 (int)
        01                             impl_menu_index= Some
        00                                               0 (nat0)
        00                                        data= Ok
        0b                                              length= 11 (int)
        04                                                body= Array: 4 items
        23                                                      0: 35 (int)
        fea6 00                                                 1: 166 (int)
        fe80 01                                                 2: 384 (int)
        fee5 03                                                 3: 997 (int)
        |}];
      let%bind _response =
        Rpc.Rpc.dispatch_exn Test_helpers.rpc conn (Bigstring.of_string "foo")
      in
      Test_helpers.Tap.print_messages tap [ [%bin_shape: Bigstring.Stable.V1.t] ];
      [%expect
        {|
        0a00 0000 0000 0000    length= 10 (64-bit LE)
        0c                       body= Query_v4
        01                             specifier= Rank
        03                                        3 (int)
        02                                    id= 2 (int)
        00                              metadata= None
        04                                  data= length= 4 (int)
        0366 6f6f                                   body= foo (3 bytes)

        0a00 0000 0000 0000    length= 10 (64-bit LE)
        08                       body= Response_v2
        02                                          id= 2 (int)
        01                             impl_menu_index= Some
        03                                               3 (nat0)
        00                                        data= Ok
        04                                              length= 4 (int)
        0366 6f6f                                         body= foo (3 bytes)
        |}];
      let%bind _response =
        Rpc.Rpc.dispatch_exn Test_helpers.rpc_v2 conn (Bigstring.of_string "foo")
      in
      Test_helpers.Tap.print_messages tap [ [%bin_shape: Bigstring.Stable.V1.t] ];
      [%expect
        {|
        0a00 0000 0000 0000    length= 10 (64-bit LE)
        0c                       body= Query_v4
        01                             specifier= Rank
        04                                        4 (int)
        03                                    id= 3 (int)
        00                              metadata= None
        04                                  data= length= 4 (int)
        0366 6f6f                                   body= foo (3 bytes)

        0a00 0000 0000 0000    length= 10 (64-bit LE)
        08                       body= Response_v2
        03                                          id= 3 (int)
        01                             impl_menu_index= Some
        04                                               4 (nat0)
        00                                        data= Ok
        04                                              length= 4 (int)
        0366 6f6f                                         body= foo (3 bytes)
        |}];
      return ())
    ()
;;

let%expect_test "forced earlier version simple RPC dispatch" =
  let env = [ "ASYNC_RPC_FORCE_PROTOCOL_VERSION_NUMBER_UPPER_BOUND", "10" ] in
  let%bind () =
    Expect_test_helpers_async.run
      ~extend_env:env
      "bin/test_forced_protocol_version.exe"
      []
  in
  (* We expect to see [Query_v3] in the output instead of later versions since we limited
     the protocol version to v10 *)
  [%expect
    {|
    ======================== header ========================
    1000 0000 0000 0000    length= 16 (64-bit LE)
    0b                       body= List: 11 items
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
    5000 0000 0000 0000    length= 80 (64-bit LE)
    07                       body= Metadata_v2
    00                             identification= None
    01                                       menu= Some
    06                                              descriptions= Array: 6 items
    0473 6f72 74                                                  0:    name= sort (4 bytes)
    01                                                               version= 1 (int)
    1074 6573 742d 6f6e    ...
    652d 7761 792d 7270    ...
    63                                                            1:    name= test-on... (16 bytes)
    01                                                               version= 1 (int)
    0d74 6573 742d 7069    ...
    7065 2d72 7063                                                2:    name= test-pi... (13 bytes)
    01                                                               version= 1 (int)
    0874 6573 742d 7270    ...
    63                                                            3:    name= test-rpc (8 bytes)
    01                                                               version= 1 (int)
    0874 6573 742d 7270    ...
    63                                                            4:    name= test-rpc (8 bytes)
    02                                                               version= 2 (int)
    0e74 6573 742d 7374    ...
    6174 652d 7270 63                                             5:    name= test-st... (14 bytes)
    01                                                               version= 1 (int)
    00                                                   digests= None
    ======================== payload messages ========================
    1400 0000 0000 0000    length= 20 (64-bit LE)
    09                       body= Query_v3
    0473 6f72 74                        tag= sort (4 bytes)
    01                              version= 1 (int)
    01                                   id= 1 (int)
    00                             metadata= None
    0a                                 data= length= 10 (int)
    03                                         body= Array: 3 items
    fe66 02                                          0: 614 (int)
    fe00 02                                          1: 512 (int)
    fe79 03                                          2: 889 (int)
    0100 0000 0000 0000    length= 1 (64-bit LE)
    00                       body= Heartbeat
    1000 0000 0000 0000    length= 16 (64-bit LE)
    08                       body= Response_v2
    01                                          id= 1 (int)
    01                             impl_menu_index= Some
    00                                               0 (nat0)
    00                                        data= Ok
    0a                                              length= 10 (int)
    03                                                body= Array: 3 items
    fe00 02                                                 0: 512 (int)
    fe66 02                                                 1: 614 (int)
    fe79 03                                                 2: 889 (int)
    |}];
  return ()
;;

let%expect_test "latest version pipe RPC dispatches" =
  Test_helpers.with_circular_connection
    ~header:Test_helpers.Header.latest
    ~f:(fun conn tap ->
      print_header tap;
      [%expect
        {|
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

        5000 0000 0000 0000    length= 80 (64-bit LE)
        07                       body= Metadata_v2
        00                             identification= None
        01                                       menu= Some
        06                                              descriptions= Array: 6 items
        0473 6f72 74                                                  0:    name= sort (4 bytes)
        01                                                               version= 1 (int)
        1074 6573 742d 6f6e    ...
        652d 7761 792d 7270    ...
        63                                                            1:    name= test-on... (16 bytes)
        01                                                               version= 1 (int)
        0d74 6573 742d 7069    ...
        7065 2d72 7063                                                2:    name= test-pi... (13 bytes)
        01                                                               version= 1 (int)
        0874 6573 742d 7270    ...
        63                                                            3:    name= test-rpc (8 bytes)
        01                                                               version= 1 (int)
        0874 6573 742d 7270    ...
        63                                                            4:    name= test-rpc (8 bytes)
        02                                                               version= 2 (int)
        0e74 6573 742d 7374    ...
        6174 652d 7270 63                                             5:    name= test-st... (14 bytes)
        01                                                               version= 1 (int)
        00                                                   digests= None
        |}];
      let%bind pipe, (_ : Rpc.Pipe_rpc.Metadata.t) =
        Rpc.Pipe_rpc.dispatch_exn Test_helpers.pipe_rpc conn (Bigstring.of_string "foo")
      in
      let%bind _messages = Pipe.to_list pipe in
      print_stream_messages tap;
      [%expect
        {|
        0f00 0000 0000 0000    length= 15 (64-bit LE)
        0c                       body= Query_v4
        01                             specifier= Rank
        02                                        2 (int)
        04                                    id= 4 (int)
        00                              metadata= None
        09                                  data= length= 9 (int)
        d1f5 2fe2                                   body= `Query
        04                                          body= length= 4 (int)
        0366 6f6f                                           body= foo (3 bytes)

        0100 0000 0000 0000    length= 1 (64-bit LE)
        00                       body= Heartbeat

        0900 0000 0000 0000    length= 9 (64-bit LE)
        08                       body= Response_v2
        04                                          id= 4 (int)
        01                             impl_menu_index= Some
        02                                               2 (nat0)
        00                                        data= Ok
        03                                              length= 3 (int)
        00                                                body= unused_query_id= 0 (int)
        00                                                              initial= Ok
        00                                                                       ()

        1400 0000 0000 0000    length= 20 (64-bit LE)
        08                       body= Response_v2
        04                                          id= 4 (int)
        01                             impl_menu_index= Some
        02                                               2 (nat0)
        00                                        data= Ok
        0e                                              length= 14 (int)
        798a 0000                                         body= `Ok
        09                                                body= length= 9 (int)
        0872 6573 706f 6e73    ...
        65                                                        body= response (8 bytes)

        1400 0000 0000 0000    length= 20 (64-bit LE)
        08                       body= Response_v2
        04                                          id= 4 (int)
        01                             impl_menu_index= Some
        02                                               2 (nat0)
        00                                        data= Ok
        0e                                              length= 14 (int)
        798a 0000                                         body= `Ok
        09                                                body= length= 9 (int)
        0872 6573 706f 6e73    ...
        65                                                        body= response (8 bytes)

        0a00 0000 0000 0000    length= 10 (64-bit LE)
        08                       body= Response_v2
        04                                          id= 4 (int)
        01                             impl_menu_index= Some
        02                                               2 (nat0)
        00                                        data= Ok
        04                                              length= 4 (int)
        3979 6900                                         body= `Eof
        |}];
      return ())
    ()
;;

let%expect_test "pipe RPC Response_v1" =
  Test_helpers.with_circular_connection
    ~header:Test_helpers.Header.v6
    ~f:(fun conn tap ->
      print_header tap;
      [%expect
        {|
        0c00 0000 0000 0000    length= 12 (64-bit LE)
        07                       body= List: 7 items
        fd52 5043 00                   0: 4411474 (int)
        01                             1: 1 (int)
        02                             2: 2 (int)
        03                             3: 3 (int)
        04                             4: 4 (int)
        05                             5: 5 (int)
        06                             6: 6 (int)

        5000 0000 0000 0000    length= 80 (64-bit LE)
        07                       body= Metadata_v2
        00                             identification= None
        01                                       menu= Some
        06                                              descriptions= Array: 6 items
        0473 6f72 74                                                  0:    name= sort (4 bytes)
        01                                                               version= 1 (int)
        1074 6573 742d 6f6e    ...
        652d 7761 792d 7270    ...
        63                                                            1:    name= test-on... (16 bytes)
        01                                                               version= 1 (int)
        0d74 6573 742d 7069    ...
        7065 2d72 7063                                                2:    name= test-pi... (13 bytes)
        01                                                               version= 1 (int)
        0874 6573 742d 7270    ...
        63                                                            3:    name= test-rpc (8 bytes)
        01                                                               version= 1 (int)
        0874 6573 742d 7270    ...
        63                                                            4:    name= test-rpc (8 bytes)
        02                                                               version= 2 (int)
        0e74 6573 742d 7374    ...
        6174 652d 7270 63                                             5:    name= test-st... (14 bytes)
        01                                                               version= 1 (int)
        00                                                   digests= None
        |}];
      let%bind pipe, (_ : Rpc.Pipe_rpc.Metadata.t) =
        Rpc.Pipe_rpc.dispatch_exn Test_helpers.pipe_rpc conn (Bigstring.of_string "foo")
      in
      let%bind _messages = Pipe.to_list pipe in
      print_stream_messages tap;
      [%expect
        {|
        1c00 0000 0000 0000    length= 28 (64-bit LE)
        03                       body= Query_v2
        0d74 6573 742d 7069    ...
        7065 2d72 7063                      tag= test-pi... (13 bytes)
        01                              version= 1 (int)
        05                                   id= 5 (int)
        00                             metadata= None
        09                                 data= length= 9 (int)
        d1f5 2fe2                                  body= `Query
        04                                         body= length= 4 (int)
        0366 6f6f                                          body= foo (3 bytes)

        0100 0000 0000 0000    length= 1 (64-bit LE)
        00                       body= Heartbeat

        0700 0000 0000 0000    length= 7 (64-bit LE)
        02                       body= Response_v1
        05                               id= 5 (int)
        00                             data= Ok
        03                                   length= 3 (int)
        00                                     body= unused_query_id= 0 (int)
        00                                                   initial= Ok
        00                                                            ()

        1200 0000 0000 0000    length= 18 (64-bit LE)
        02                       body= Response_v1
        05                               id= 5 (int)
        00                             data= Ok
        0e                                   length= 14 (int)
        798a 0000                              body= `Ok
        09                                     body= length= 9 (int)
        0872 6573 706f 6e73    ...
        65                                             body= response (8 bytes)

        1200 0000 0000 0000    length= 18 (64-bit LE)
        02                       body= Response_v1
        05                               id= 5 (int)
        00                             data= Ok
        0e                                   length= 14 (int)
        798a 0000                              body= `Ok
        09                                     body= length= 9 (int)
        0872 6573 706f 6e73    ...
        65                                             body= response (8 bytes)

        0800 0000 0000 0000    length= 8 (64-bit LE)
        02                       body= Response_v1
        05                               id= 5 (int)
        00                             data= Ok
        04                                   length= 4 (int)
        3979 6900                              body= `Eof
        |}];
      return ())
    ()
;;

let%expect_test "V3 send versioned menu automatically" =
  let%bind () =
    connection_test
      ()
      ~provide_rpc_shapes:true
      ~server_header:Test_helpers.Header.v3
      ~client_header:Test_helpers.Header.v3
      ~f:connection_test_menu
  in
  [%expect
    {|
    (client_menu ())
    (server_menu
     (((name sort)
       (versions
        (((version 1)
          (digest
           (Rpc (query 4c138035aa69ec9dd8b7a7119090f84a)
            (response 4c138035aa69ec9dd8b7a7119090f84a)))))))
      ((name test-one-way-rpc)
       (versions
        (((version 1) (digest (One_way (msg e2d261c6c291b94bf6aa68ec2b08cb00)))))))
      ((name test-pipe-rpc)
       (versions
        (((version 1)
          (digest
           (Streaming_rpc (query e2d261c6c291b94bf6aa68ec2b08cb00)
            (initial_response 86ba5df747eec837f0b391dd49f33f9e)
            (update_response e2d261c6c291b94bf6aa68ec2b08cb00)
            (error 52966f4a49a77bfdff668e9cc61511b3)))))))
      ((name test-rpc)
       (versions
        (((version 1)
          (digest
           (Rpc (query e2d261c6c291b94bf6aa68ec2b08cb00)
            (response e2d261c6c291b94bf6aa68ec2b08cb00))))
         ((version 2)
          (digest
           (Rpc (query e2d261c6c291b94bf6aa68ec2b08cb00)
            (response e2d261c6c291b94bf6aa68ec2b08cb00)))))))
      ((name test-state-rpc)
       (versions
        (((version 1)
          (digest
           (Streaming_rpc (query e2d261c6c291b94bf6aa68ec2b08cb00)
            (initial_response e2d261c6c291b94bf6aa68ec2b08cb00)
            (update_response e2d261c6c291b94bf6aa68ec2b08cb00)
            (error 52966f4a49a77bfdff668e9cc61511b3)))))))))
    |}];
  let%bind () =
    connection_test
      ()
      ~provide_rpc_shapes:true
      ~server_header:Test_helpers.Header.v3
      ~client_header:Test_helpers.Header.v2
      ~f:connection_test_menu
  in
  (* We send no menu to v1 or v2. *)
  [%expect {| "No menus received" |}];
  return ()
;;

let%expect_test "V6 sometimes sends shapes in the menu" =
  (* v6 -> v6 with [provide_rpc_shapes:true]. We expect real digests to be sent. *)
  let%bind () =
    connection_test
      ()
      ~provide_rpc_shapes:true
      ~server_header:Test_helpers.Header.v6
      ~client_header:Test_helpers.Header.v6
      ~f:connection_test_menu
  in
  [%expect
    {|
    (client_menu ())
    (server_menu
     (((name sort)
       (versions
        (((version 1)
          (digest
           (Rpc (query 4c138035aa69ec9dd8b7a7119090f84a)
            (response 4c138035aa69ec9dd8b7a7119090f84a)))))))
      ((name test-one-way-rpc)
       (versions
        (((version 1) (digest (One_way (msg e2d261c6c291b94bf6aa68ec2b08cb00)))))))
      ((name test-pipe-rpc)
       (versions
        (((version 1)
          (digest
           (Streaming_rpc (query e2d261c6c291b94bf6aa68ec2b08cb00)
            (initial_response 86ba5df747eec837f0b391dd49f33f9e)
            (update_response e2d261c6c291b94bf6aa68ec2b08cb00)
            (error 52966f4a49a77bfdff668e9cc61511b3)))))))
      ((name test-rpc)
       (versions
        (((version 1)
          (digest
           (Rpc (query e2d261c6c291b94bf6aa68ec2b08cb00)
            (response e2d261c6c291b94bf6aa68ec2b08cb00))))
         ((version 2)
          (digest
           (Rpc (query e2d261c6c291b94bf6aa68ec2b08cb00)
            (response e2d261c6c291b94bf6aa68ec2b08cb00)))))))
      ((name test-state-rpc)
       (versions
        (((version 1)
          (digest
           (Streaming_rpc (query e2d261c6c291b94bf6aa68ec2b08cb00)
            (initial_response e2d261c6c291b94bf6aa68ec2b08cb00)
            (update_response e2d261c6c291b94bf6aa68ec2b08cb00)
            (error 52966f4a49a77bfdff668e9cc61511b3)))))))))
    |}];
  (* v6 -> v6 with [provide_rpc_shapes:false]. We don't send [Unknown] digests in the menu
     (which a [Menu.Stable.V3.response option]) *)
  let%bind () =
    connection_test
      ()
      ~provide_rpc_shapes:false
      ~server_header:Test_helpers.Header.v6
      ~client_header:Test_helpers.Header.v6
      ~f:connection_test_menu
  in
  [%expect
    {|
    (client_menu ())
    (server_menu
     (((name sort) (versions (((version 1) (digest Unknown)))))
      ((name test-one-way-rpc) (versions (((version 1) (digest Unknown)))))
      ((name test-pipe-rpc) (versions (((version 1) (digest Unknown)))))
      ((name test-rpc)
       (versions (((version 1) (digest Unknown)) ((version 2) (digest Unknown)))))
      ((name test-state-rpc) (versions (((version 1) (digest Unknown)))))))
    |}];
  (* v6 -> v3, we must send rpc shapes still but we explicitly send [Unknown], breaking
     backwards compatibility. This looks the same as the v6 output but the difference is
     that we're sending a [Menu.Stable.V2.response option]. So it's useful to test that
     this looks the same as v6 -> v6 *)
  let%bind () =
    connection_test
      ()
      ~provide_rpc_shapes:false
      ~server_header:Test_helpers.Header.v6
      ~client_header:Test_helpers.Header.v3
      ~f:connection_test_menu
  in
  [%expect
    {|
    (client_menu ())
    (server_menu
     (((name sort) (versions (((version 1) (digest Unknown)))))
      ((name test-one-way-rpc) (versions (((version 1) (digest Unknown)))))
      ((name test-pipe-rpc) (versions (((version 1) (digest Unknown)))))
      ((name test-rpc)
       (versions (((version 1) (digest Unknown)) ((version 2) (digest Unknown)))))
      ((name test-state-rpc) (versions (((version 1) (digest Unknown)))))))
    |}];
  (* v6 -> v2, we don't send any menu *)
  let%bind () =
    connection_test
      ()
      ~server_header:Test_helpers.Header.v6
      ~client_header:Test_helpers.Header.v2
      ~f:connection_test_menu
  in
  [%expect {| "No menus received" |}];
  return ()
;;

let%expect_test "V3 identification string addition" =
  let%bind () =
    connection_test
      ()
      ~server_header:Test_helpers.Header.v3
      ~client_header:Test_helpers.Header.v3
      ~f:connection_test_id
  in
  [%expect
    {|
    ---   client -> server:   ---
    0900 0000 0000 0000    length= 9 (64-bit LE)
    04                       body= List: 4 items
    fd52 5043 00                   0: 4411474 (int)
    01                             1: 1 (int)
    02                             2: 2 (int)
    03                             3: 3 (int)

    0c00 0000 0000 0000    length= 12 (64-bit LE)
    04                       body= Metadata
    01                             identification= Some
    0763 6c69 6e2d 6964                             clin-id (7 bytes)
    01                                       menu= Some
    00                                              List: 0 items
    ---   server -> client:   ---
    0900 0000 0000 0000    length= 9 (64-bit LE)
    04                       body= List: 4 items
    fd52 5043 00                   0: 4411474 (int)
    01                             1: 1 (int)
    02                             2: 2 (int)
    03                             3: 3 (int)

    5d00 0000 0000 0000    length= 93 (64-bit LE)
    04                       body= Metadata
    01                             identification= Some
    0773 6572 762d 6964                             serv-id (7 bytes)
    01                                       menu= Some
    06                                              List: 6 items
    0473 6f72 74                                    0: 1.    name= sort (4 bytes)
    01                                                    version= 1 (int)
    03                                                 2. Unknown
    1074 6573 742d 6f6e    ...
    652d 7761 792d 7270    ...
    63                                              1: 1.    name= test-on... (16 bytes)
    01                                                    version= 1 (int)
    03                                                 2. Unknown
    0d74 6573 742d 7069    ...
    7065 2d72 7063                                  2: 1.    name= test-pi... (13 bytes)
    01                                                    version= 1 (int)
    03                                                 2. Unknown
    0874 6573 742d 7270    ...
    63                                              3: 1.    name= test-rpc (8 bytes)
    01                                                    version= 1 (int)
    03                                                 2. Unknown
    0874 6573 742d 7270    ...
    63                                              4: 1.    name= test-rpc (8 bytes)
    02                                                    version= 2 (int)
    03                                                 2. Unknown
    0e74 6573 742d 7374    ...
    6174 652d 7270 63                               5: 1.    name= test-st... (14 bytes)
    01                                                    version= 1 (int)
    03                                                 2. Unknown
    ---   client -> server:   ---
    ---   server -> client:   ---
    ((server_id_from_client (serv-id)) (client_id_from_server (clin-id)))
    |}];
  let%bind () =
    connection_test
      ()
      ~server_header:Test_helpers.Header.v3
      ~client_header:Test_helpers.Header.v2
      ~f:connection_test_id
  in
  [%expect
    {|
    ---   client -> server:   ---
    0800 0000 0000 0000    length= 8 (64-bit LE)
    03                       body= List: 3 items
    fd52 5043 00                   0: 4411474 (int)
    01                             1: 1 (int)
    02                             2: 2 (int)
    ---   server -> client:   ---
    0900 0000 0000 0000    length= 9 (64-bit LE)
    04                       body= List: 4 items
    fd52 5043 00                   0: 4411474 (int)
    01                             1: 1 (int)
    02                             2: 2 (int)
    03                             3: 3 (int)
    ---   client -> server:   ---
    ---   server -> client:   ---
    ((server_id_from_client ()) (client_id_from_server ()))
    |}];
  let%bind () =
    connection_test
      ()
      ~server_header:Test_helpers.Header.v1
      ~client_header:Test_helpers.Header.v3
      ~f:connection_test_id
  in
  [%expect
    {|
    ---   client -> server:   ---
    0900 0000 0000 0000    length= 9 (64-bit LE)
    04                       body= List: 4 items
    fd52 5043 00                   0: 4411474 (int)
    01                             1: 1 (int)
    02                             2: 2 (int)
    03                             3: 3 (int)
    ---   server -> client:   ---
    0700 0000 0000 0000    length= 7 (64-bit LE)
    02                       body= List: 2 items
    fd52 5043 00                   0: 4411474 (int)
    01                             1: 1 (int)
    ---   client -> server:   ---
    ---   server -> client:   ---
    ((server_id_from_client ()) (client_id_from_server ()))
    |}];
  return ()
;;

let%expect_test "V4 close reason addition" =
  let connection_test_with_close ~client ~server ~s_to_c ~c_to_s =
    print_headers ~s_to_c ~c_to_s;
    let%bind server_id_from_client = Rpc.Connection.peer_identification client in
    let%bind client_id_from_server = Rpc.Connection.peer_identification server in
    let%map () =
      Rpc.Connection.close client ~reason:(Info.create_s [%message "test reason"])
    in
    print_payload_messages_bidirectional ~s_to_c ~c_to_s;
    print_s
      [%message
        (server_id_from_client : Bigstring.t option)
          (client_id_from_server : Bigstring.t option)]
  in
  let%bind () =
    Test_helpers.with_rpc_server_connection
      ()
      ~server_header:Test_helpers.Header.v4
      ~client_header:Test_helpers.Header.v4
      ~f:connection_test_with_close
  in
  [%expect
    {|
    ---   client -> server:   ---
    0a00 0000 0000 0000    length= 10 (64-bit LE)
    05                       body= List: 5 items
    fd52 5043 00                   0: 4411474 (int)
    01                             1: 1 (int)
    02                             2: 2 (int)
    03                             3: 3 (int)
    04                             4: 4 (int)

    0c00 0000 0000 0000    length= 12 (64-bit LE)
    04                       body= Metadata
    01                             identification= Some
    0763 6c69 6e2d 6964                             clin-id (7 bytes)
    01                                       menu= Some
    00                                              List: 0 items
    ---   server -> client:   ---
    0a00 0000 0000 0000    length= 10 (64-bit LE)
    05                       body= List: 5 items
    fd52 5043 00                   0: 4411474 (int)
    01                             1: 1 (int)
    02                             2: 2 (int)
    03                             3: 3 (int)
    04                             4: 4 (int)

    5d00 0000 0000 0000    length= 93 (64-bit LE)
    04                       body= Metadata
    01                             identification= Some
    0773 6572 762d 6964                             serv-id (7 bytes)
    01                                       menu= Some
    06                                              List: 6 items
    0473 6f72 74                                    0: 1.    name= sort (4 bytes)
    01                                                    version= 1 (int)
    03                                                 2. Unknown
    1074 6573 742d 6f6e    ...
    652d 7761 792d 7270    ...
    63                                              1: 1.    name= test-on... (16 bytes)
    01                                                    version= 1 (int)
    03                                                 2. Unknown
    0d74 6573 742d 7069    ...
    7065 2d72 7063                                  2: 1.    name= test-pi... (13 bytes)
    01                                                    version= 1 (int)
    03                                                 2. Unknown
    0874 6573 742d 7270    ...
    63                                              3: 1.    name= test-rpc (8 bytes)
    01                                                    version= 1 (int)
    03                                                 2. Unknown
    0874 6573 742d 7270    ...
    63                                              4: 1.    name= test-rpc (8 bytes)
    02                                                    version= 2 (int)
    03                                                 2. Unknown
    0e74 6573 742d 7374    ...
    6174 652d 7270 63                               5: 1.    name= test-st... (14 bytes)
    01                                                    version= 1 (int)
    03                                                 2. Unknown
    ---   client -> server:   ---
    0f00 0000 0000 0000    length= 15 (64-bit LE)
    05                       body= Close_reason
    03                               Sexp
    00                                 Atom
    0b74 6573 7420 7265    ...
    6173 6f6e                          "test re..." (11 bytes)
    ---   server -> client:   ---
    0100 0000 0000 0000    length= 1 (64-bit LE)
    00                       body= Heartbeat
    ((server_id_from_client (serv-id)) (client_id_from_server (clin-id)))
    |}];
  let%bind () =
    Test_helpers.with_rpc_server_connection
      ()
      ~server_header:Test_helpers.Header.v3
      ~client_header:Test_helpers.Header.v4
      ~f:connection_test_with_close
  in
  [%expect
    {|
    ---   client -> server:   ---
    0a00 0000 0000 0000    length= 10 (64-bit LE)
    05                       body= List: 5 items
    fd52 5043 00                   0: 4411474 (int)
    01                             1: 1 (int)
    02                             2: 2 (int)
    03                             3: 3 (int)
    04                             4: 4 (int)

    0c00 0000 0000 0000    length= 12 (64-bit LE)
    04                       body= Metadata
    01                             identification= Some
    0763 6c69 6e2d 6964                             clin-id (7 bytes)
    01                                       menu= Some
    00                                              List: 0 items
    ---   server -> client:   ---
    0900 0000 0000 0000    length= 9 (64-bit LE)
    04                       body= List: 4 items
    fd52 5043 00                   0: 4411474 (int)
    01                             1: 1 (int)
    02                             2: 2 (int)
    03                             3: 3 (int)

    5d00 0000 0000 0000    length= 93 (64-bit LE)
    04                       body= Metadata
    01                             identification= Some
    0773 6572 762d 6964                             serv-id (7 bytes)
    01                                       menu= Some
    06                                              List: 6 items
    0473 6f72 74                                    0: 1.    name= sort (4 bytes)
    01                                                    version= 1 (int)
    03                                                 2. Unknown
    1074 6573 742d 6f6e    ...
    652d 7761 792d 7270    ...
    63                                              1: 1.    name= test-on... (16 bytes)
    01                                                    version= 1 (int)
    03                                                 2. Unknown
    0d74 6573 742d 7069    ...
    7065 2d72 7063                                  2: 1.    name= test-pi... (13 bytes)
    01                                                    version= 1 (int)
    03                                                 2. Unknown
    0874 6573 742d 7270    ...
    63                                              3: 1.    name= test-rpc (8 bytes)
    01                                                    version= 1 (int)
    03                                                 2. Unknown
    0874 6573 742d 7270    ...
    63                                              4: 1.    name= test-rpc (8 bytes)
    02                                                    version= 2 (int)
    03                                                 2. Unknown
    0e74 6573 742d 7374    ...
    6174 652d 7270 63                               5: 1.    name= test-st... (14 bytes)
    01                                                    version= 1 (int)
    03                                                 2. Unknown
    ---   client -> server:   ---
    ---   server -> client:   ---
    0100 0000 0000 0000    length= 1 (64-bit LE)
    00                       body= Heartbeat
    ((server_id_from_client (serv-id)) (client_id_from_server (clin-id)))
    |}];
  return ()
;;

let%expect_test "V9 close started addition" =
  let connection_test_with_close ~client ~server ~s_to_c ~c_to_s =
    print_headers ~s_to_c ~c_to_s;
    let%bind server_id_from_client = Rpc.Connection.peer_identification client in
    let%bind client_id_from_server = Rpc.Connection.peer_identification server in
    let%map () =
      Rpc.Connection.close
        client
        ~reason:(Info.create_s [%message "test reason"])
        ~wait_for_open_queries_timeout:(Time_ns.Span.of_sec 0.0)
    in
    print_payload_messages_bidirectional ~s_to_c ~c_to_s;
    print_s
      [%message
        (server_id_from_client : Bigstring.t option)
          (client_id_from_server : Bigstring.t option)]
  in
  let%bind () =
    Test_helpers.with_rpc_server_connection
      ()
      ~server_header:Test_helpers.Header.v9
      ~client_header:Test_helpers.Header.v9
      ~f:connection_test_with_close
  in
  (* We expect there to be a [Close_started] message sent before the [Close_reason] *)
  [%expect
    {|
    ---   client -> server:   ---
    0f00 0000 0000 0000    length= 15 (64-bit LE)
    0a                       body= List: 10 items
    fd52 5043 00                   0: 4411474 (int)
    01                             1: 1 (int)
    02                             2: 2 (int)
    03                             3: 3 (int)
    04                             4: 4 (int)
    05                             5: 5 (int)
    06                             6: 6 (int)
    07                             7: 7 (int)
    08                             8: 8 (int)
    09                             9: 9 (int)

    0d00 0000 0000 0000    length= 13 (64-bit LE)
    07                       body= Metadata_v2
    01                             identification= Some
    0763 6c69 6e2d 6964                             clin-id (7 bytes)
    01                                       menu= Some
    00                                              descriptions= Array: 0 items
    00                                                   digests= None
    ---   server -> client:   ---
    0f00 0000 0000 0000    length= 15 (64-bit LE)
    0a                       body= List: 10 items
    fd52 5043 00                   0: 4411474 (int)
    01                             1: 1 (int)
    02                             2: 2 (int)
    03                             3: 3 (int)
    04                             4: 4 (int)
    05                             5: 5 (int)
    06                             6: 6 (int)
    07                             7: 7 (int)
    08                             8: 8 (int)
    09                             9: 9 (int)

    5800 0000 0000 0000    length= 88 (64-bit LE)
    07                       body= Metadata_v2
    01                             identification= Some
    0773 6572 762d 6964                             serv-id (7 bytes)
    01                                       menu= Some
    06                                              descriptions= Array: 6 items
    0473 6f72 74                                                  0:    name= sort (4 bytes)
    01                                                               version= 1 (int)
    1074 6573 742d 6f6e    ...
    652d 7761 792d 7270    ...
    63                                                            1:    name= test-on... (16 bytes)
    01                                                               version= 1 (int)
    0d74 6573 742d 7069    ...
    7065 2d72 7063                                                2:    name= test-pi... (13 bytes)
    01                                                               version= 1 (int)
    0874 6573 742d 7270    ...
    63                                                            3:    name= test-rpc (8 bytes)
    01                                                               version= 1 (int)
    0874 6573 742d 7270    ...
    63                                                            4:    name= test-rpc (8 bytes)
    02                                                               version= 2 (int)
    0e74 6573 742d 7374    ...
    6174 652d 7270 63                                             5:    name= test-st... (14 bytes)
    01                                                               version= 1 (int)
    00                                                   digests= None
    ---   client -> server:   ---
    0100 0000 0000 0000    length= 1 (64-bit LE)
    0a                       body= Close_started

    0f00 0000 0000 0000    length= 15 (64-bit LE)
    05                       body= Close_reason
    03                               Sexp
    00                                 Atom
    0b74 6573 7420 7265    ...
    6173 6f6e                          "test re..." (11 bytes)
    ---   server -> client:   ---
    0100 0000 0000 0000    length= 1 (64-bit LE)
    00                       body= Heartbeat
    ((server_id_from_client (serv-id)) (client_id_from_server (clin-id)))
    |}];
  let%bind () =
    Test_helpers.with_rpc_server_connection
      ()
      ~server_header:Test_helpers.Header.v8
      ~client_header:Test_helpers.Header.v9
      ~f:connection_test_with_close
  in
  (* We expect there to only be a [Close_reason] message sent *)
  [%expect
    {|
    ---   client -> server:   ---
    0f00 0000 0000 0000    length= 15 (64-bit LE)
    0a                       body= List: 10 items
    fd52 5043 00                   0: 4411474 (int)
    01                             1: 1 (int)
    02                             2: 2 (int)
    03                             3: 3 (int)
    04                             4: 4 (int)
    05                             5: 5 (int)
    06                             6: 6 (int)
    07                             7: 7 (int)
    08                             8: 8 (int)
    09                             9: 9 (int)

    0d00 0000 0000 0000    length= 13 (64-bit LE)
    07                       body= Metadata_v2
    01                             identification= Some
    0763 6c69 6e2d 6964                             clin-id (7 bytes)
    01                                       menu= Some
    00                                              descriptions= Array: 0 items
    00                                                   digests= None
    ---   server -> client:   ---
    0e00 0000 0000 0000    length= 14 (64-bit LE)
    09                       body= List: 9 items
    fd52 5043 00                   0: 4411474 (int)
    01                             1: 1 (int)
    02                             2: 2 (int)
    03                             3: 3 (int)
    04                             4: 4 (int)
    05                             5: 5 (int)
    06                             6: 6 (int)
    07                             7: 7 (int)
    08                             8: 8 (int)

    5800 0000 0000 0000    length= 88 (64-bit LE)
    07                       body= Metadata_v2
    01                             identification= Some
    0773 6572 762d 6964                             serv-id (7 bytes)
    01                                       menu= Some
    06                                              descriptions= Array: 6 items
    0473 6f72 74                                                  0:    name= sort (4 bytes)
    01                                                               version= 1 (int)
    1074 6573 742d 6f6e    ...
    652d 7761 792d 7270    ...
    63                                                            1:    name= test-on... (16 bytes)
    01                                                               version= 1 (int)
    0d74 6573 742d 7069    ...
    7065 2d72 7063                                                2:    name= test-pi... (13 bytes)
    01                                                               version= 1 (int)
    0874 6573 742d 7270    ...
    63                                                            3:    name= test-rpc (8 bytes)
    01                                                               version= 1 (int)
    0874 6573 742d 7270    ...
    63                                                            4:    name= test-rpc (8 bytes)
    02                                                               version= 2 (int)
    0e74 6573 742d 7374    ...
    6174 652d 7270 63                                             5:    name= test-st... (14 bytes)
    01                                                               version= 1 (int)
    00                                                   digests= None
    ---   client -> server:   ---
    0f00 0000 0000 0000    length= 15 (64-bit LE)
    05                       body= Close_reason
    03                               Sexp
    00                                 Atom
    0b74 6573 7420 7265    ...
    6173 6f6e                          "test re..." (11 bytes)
    ---   server -> client:   ---
    0100 0000 0000 0000    length= 1 (64-bit LE)
    00                       body= Heartbeat
    ((server_id_from_client (serv-id)) (client_id_from_server (clin-id)))
    |}];
  return ()
;;

let%expect_test "V2 local rpc" =
  Test_helpers.with_circular_connection
    ~header:Test_helpers.Header.v2
    ~f:(fun conn tap ->
      print_header tap;
      [%expect
        {|
        0800 0000 0000 0000    length= 8 (64-bit LE)
        03                       body= List: 3 items
        fd52 5043 00                   0: 4411474 (int)
        01                             1: 1 (int)
        02                             2: 2 (int)
        |}];
      let payload = Payload.create () in
      let rpc = Test_helpers.sort_rpc in
      let%bind _response = Rpc.Rpc.dispatch_exn rpc conn payload in
      print_payload_messages tap;
      [%expect
        {|
        1500 0000 0000 0000    length= 21 (64-bit LE)
        03                       body= Query_v2
        0473 6f72 74                        tag= sort (4 bytes)
        01                              version= 1 (int)
        06                                   id= 6 (int)
        00                             metadata= None
        0b                                 data= length= 11 (int)
        04                                         body= Array: 4 items
        fe80 01                                          0: 384 (int)
        fee5 03                                          1: 997 (int)
        fea6 00                                          2: 166 (int)
        23                                               3: 35 (int)

        0100 0000 0000 0000    length= 1 (64-bit LE)
        00                       body= Heartbeat

        0f00 0000 0000 0000    length= 15 (64-bit LE)
        02                       body= Response_v1
        06                               id= 6 (int)
        00                             data= Ok
        0b                                   length= 11 (int)
        04                                     body= Array: 4 items
        23                                           0: 35 (int)
        fea6 00                                      1: 166 (int)
        fe80 01                                      2: 384 (int)
        fee5 03                                      3: 997 (int)
        |}];
      let%bind _response = Rpc.Rpc.dispatch_exn rpc conn payload in
      print_payload_messages tap;
      [%expect
        {|
        1500 0000 0000 0000    length= 21 (64-bit LE)
        03                       body= Query_v2
        0473 6f72 74                        tag= sort (4 bytes)
        01                              version= 1 (int)
        07                                   id= 7 (int)
        00                             metadata= None
        0b                                 data= length= 11 (int)
        04                                         body= Array: 4 items
        fe80 01                                          0: 384 (int)
        fee5 03                                          1: 997 (int)
        fea6 00                                          2: 166 (int)
        23                                               3: 35 (int)

        0f00 0000 0000 0000    length= 15 (64-bit LE)
        02                       body= Response_v1
        07                               id= 7 (int)
        00                             data= Ok
        0b                                   length= 11 (int)
        04                                     body= Array: 4 items
        23                                           0: 35 (int)
        fea6 00                                      1: 166 (int)
        fe80 01                                      2: 384 (int)
        fee5 03                                      3: 997 (int)
        |}];
      let%bind _response = Rpc.Rpc.dispatch_exn rpc conn payload in
      print_payload_messages tap;
      [%expect
        {|
        1500 0000 0000 0000    length= 21 (64-bit LE)
        03                       body= Query_v2
        0473 6f72 74                        tag= sort (4 bytes)
        01                              version= 1 (int)
        08                                   id= 8 (int)
        00                             metadata= None
        0b                                 data= length= 11 (int)
        04                                         body= Array: 4 items
        fe80 01                                          0: 384 (int)
        fee5 03                                          1: 997 (int)
        fea6 00                                          2: 166 (int)
        23                                               3: 35 (int)

        0f00 0000 0000 0000    length= 15 (64-bit LE)
        02                       body= Response_v1
        08                               id= 8 (int)
        00                             data= Ok
        0b                                   length= 11 (int)
        04                                     body= Array: 4 items
        23                                           0: 35 (int)
        fea6 00                                      1: 166 (int)
        fe80 01                                      2: 384 (int)
        fee5 03                                      3: 997 (int)
        |}];
      return ())
    ()
;;

let%expect_test "V1 local rpc" =
  Test_helpers.with_circular_connection
    ~header:Test_helpers.Header.v1
    ~f:(fun conn tap ->
      print_header tap;
      [%expect
        {|
        0700 0000 0000 0000    length= 7 (64-bit LE)
        02                       body= List: 2 items
        fd52 5043 00                   0: 4411474 (int)
        01                             1: 1 (int)
        |}];
      let payload = Payload.create () in
      let%bind _response = Rpc.Rpc.dispatch_exn Test_helpers.sort_rpc conn payload in
      print_payload_messages tap;
      [%expect
        {|
        1400 0000 0000 0000    length= 20 (64-bit LE)
        01                       body= Query_v1
        0473 6f72 74                       tag= sort (4 bytes)
        01                             version= 1 (int)
        09                                  id= 9 (int)
        0b                                data= length= 11 (int)
        04                                        body= Array: 4 items
        fe80 01                                         0: 384 (int)
        fee5 03                                         1: 997 (int)
        fea6 00                                         2: 166 (int)
        23                                              3: 35 (int)

        0100 0000 0000 0000    length= 1 (64-bit LE)
        00                       body= Heartbeat

        0f00 0000 0000 0000    length= 15 (64-bit LE)
        02                       body= Response_v1
        09                               id= 9 (int)
        00                             data= Ok
        0b                                   length= 11 (int)
        04                                     body= Array: 4 items
        23                                           0: 35 (int)
        fea6 00                                      1: 166 (int)
        fe80 01                                      2: 384 (int)
        fee5 03                                      3: 997 (int)
        |}];
      let%bind _response = Rpc.Rpc.dispatch_exn Test_helpers.sort_rpc conn payload in
      print_payload_messages tap;
      [%expect
        {|
        1400 0000 0000 0000    length= 20 (64-bit LE)
        01                       body= Query_v1
        0473 6f72 74                       tag= sort (4 bytes)
        01                             version= 1 (int)
        0a                                  id= 10 (int)
        0b                                data= length= 11 (int)
        04                                        body= Array: 4 items
        fe80 01                                         0: 384 (int)
        fee5 03                                         1: 997 (int)
        fea6 00                                         2: 166 (int)
        23                                              3: 35 (int)

        0f00 0000 0000 0000    length= 15 (64-bit LE)
        02                       body= Response_v1
        0a                               id= 10 (int)
        00                             data= Ok
        0b                                   length= 11 (int)
        04                                     body= Array: 4 items
        23                                           0: 35 (int)
        fea6 00                                      1: 166 (int)
        fe80 01                                      2: 384 (int)
        fee5 03                                      3: 997 (int)
        |}];
      let%bind _response = Rpc.Rpc.dispatch_exn Test_helpers.sort_rpc conn payload in
      print_payload_messages tap;
      [%expect
        {|
        1400 0000 0000 0000    length= 20 (64-bit LE)
        01                       body= Query_v1
        0473 6f72 74                       tag= sort (4 bytes)
        01                             version= 1 (int)
        0b                                  id= 11 (int)
        0b                                data= length= 11 (int)
        04                                        body= Array: 4 items
        fe80 01                                         0: 384 (int)
        fee5 03                                         1: 997 (int)
        fea6 00                                         2: 166 (int)
        23                                              3: 35 (int)

        0f00 0000 0000 0000    length= 15 (64-bit LE)
        02                       body= Response_v1
        0b                               id= 11 (int)
        00                             data= Ok
        0b                                   length= 11 (int)
        04                                     body= Array: 4 items
        23                                           0: 35 (int)
        fea6 00                                      1: 166 (int)
        fe80 01                                      2: 384 (int)
        fee5 03                                      3: 997 (int)
        |}];
      return ())
    ()
;;

let%expect_test "[V1 -> V1] RPC connection" =
  Test_helpers.with_rpc_server_connection
    ()
    ~client_header:Test_helpers.Header.v1
    ~server_header:Test_helpers.Header.v1
    ~f:(fun ~client ~server:_ ~s_to_c ~c_to_s ->
      print_headers ~s_to_c ~c_to_s;
      [%expect
        {|
        ---   client -> server:   ---
        0700 0000 0000 0000    length= 7 (64-bit LE)
        02                       body= List: 2 items
        fd52 5043 00                   0: 4411474 (int)
        01                             1: 1 (int)
        ---   server -> client:   ---
        0700 0000 0000 0000    length= 7 (64-bit LE)
        02                       body= List: 2 items
        fd52 5043 00                   0: 4411474 (int)
        01                             1: 1 (int)
        |}];
      let%bind () = dispatch ~client in
      print_payload_messages_bidirectional ~s_to_c ~c_to_s;
      [%expect
        {|
        ---   client -> server:   ---
        1400 0000 0000 0000    length= 20 (64-bit LE)
        01                       body= Query_v1
        0473 6f72 74                       tag= sort (4 bytes)
        01                             version= 1 (int)
        0c                                  id= 12 (int)
        0b                                data= length= 11 (int)
        04                                        body= Array: 4 items
        fe80 01                                         0: 384 (int)
        fee5 03                                         1: 997 (int)
        fea6 00                                         2: 166 (int)
        23                                              3: 35 (int)

        0100 0000 0000 0000    length= 1 (64-bit LE)
        00                       body= Heartbeat
        ---   server -> client:   ---
        0100 0000 0000 0000    length= 1 (64-bit LE)
        00                       body= Heartbeat

        0f00 0000 0000 0000    length= 15 (64-bit LE)
        02                       body= Response_v1
        0c                               id= 12 (int)
        00                             data= Ok
        0b                                   length= 11 (int)
        04                                     body= Array: 4 items
        23                                           0: 35 (int)
        fea6 00                                      1: 166 (int)
        fe80 01                                      2: 384 (int)
        fee5 03                                      3: 997 (int)
        |}];
      let%bind () = dispatch ~client in
      print_payload_messages_bidirectional ~s_to_c ~c_to_s;
      [%expect
        {|
        ---   client -> server:   ---
        0a00 0000 0000 0000    length= 10 (64-bit LE)
        01                       body= Query_v1
        0473 6f72 74                       tag= sort (4 bytes)
        01                             version= 1 (int)
        0d                                  id= 13 (int)
        01                                data= length= 1 (int)
        00                                        body= Array: 0 items
        ---   server -> client:   ---
        0500 0000 0000 0000    length= 5 (64-bit LE)
        02                       body= Response_v1
        0d                               id= 13 (int)
        00                             data= Ok
        01                                   length= 1 (int)
        00                                     body= Array: 0 items
        |}];
      let%bind () = dispatch ~client in
      print_payload_messages_bidirectional ~s_to_c ~c_to_s;
      [%expect
        {|
        ---   client -> server:   ---
        0a00 0000 0000 0000    length= 10 (64-bit LE)
        01                       body= Query_v1
        0473 6f72 74                       tag= sort (4 bytes)
        01                             version= 1 (int)
        0e                                  id= 14 (int)
        01                                data= length= 1 (int)
        00                                        body= Array: 0 items
        ---   server -> client:   ---
        0500 0000 0000 0000    length= 5 (64-bit LE)
        02                       body= Response_v1
        0e                               id= 14 (int)
        00                             data= Ok
        01                                   length= 1 (int)
        00                                     body= Array: 0 items
        |}];
      return ())
;;

let%expect_test "[V2 -> V2] RPC connection" =
  Test_helpers.with_rpc_server_connection
    ()
    ~client_header:Test_helpers.Header.v2
    ~server_header:Test_helpers.Header.v2
    ~f:(fun ~client ~server:_ ~s_to_c ~c_to_s ->
      print_headers ~s_to_c ~c_to_s;
      [%expect
        {|
        ---   client -> server:   ---
        0800 0000 0000 0000    length= 8 (64-bit LE)
        03                       body= List: 3 items
        fd52 5043 00                   0: 4411474 (int)
        01                             1: 1 (int)
        02                             2: 2 (int)
        ---   server -> client:   ---
        0800 0000 0000 0000    length= 8 (64-bit LE)
        03                       body= List: 3 items
        fd52 5043 00                   0: 4411474 (int)
        01                             1: 1 (int)
        02                             2: 2 (int)
        |}];
      let%bind () = dispatch ~client in
      print_payload_messages_bidirectional ~s_to_c ~c_to_s;
      [%expect
        {|
        ---   client -> server:   ---
        1500 0000 0000 0000    length= 21 (64-bit LE)
        03                       body= Query_v2
        0473 6f72 74                        tag= sort (4 bytes)
        01                              version= 1 (int)
        0f                                   id= 15 (int)
        00                             metadata= None
        0b                                 data= length= 11 (int)
        04                                         body= Array: 4 items
        fe80 01                                          0: 384 (int)
        fee5 03                                          1: 997 (int)
        fea6 00                                          2: 166 (int)
        23                                               3: 35 (int)

        0100 0000 0000 0000    length= 1 (64-bit LE)
        00                       body= Heartbeat
        ---   server -> client:   ---
        0100 0000 0000 0000    length= 1 (64-bit LE)
        00                       body= Heartbeat

        0f00 0000 0000 0000    length= 15 (64-bit LE)
        02                       body= Response_v1
        0f                               id= 15 (int)
        00                             data= Ok
        0b                                   length= 11 (int)
        04                                     body= Array: 4 items
        23                                           0: 35 (int)
        fea6 00                                      1: 166 (int)
        fe80 01                                      2: 384 (int)
        fee5 03                                      3: 997 (int)
        |}];
      let%bind () = dispatch ~client in
      print_payload_messages_bidirectional ~s_to_c ~c_to_s;
      [%expect
        {|
        ---   client -> server:   ---
        0b00 0000 0000 0000    length= 11 (64-bit LE)
        03                       body= Query_v2
        0473 6f72 74                        tag= sort (4 bytes)
        01                              version= 1 (int)
        10                                   id= 16 (int)
        00                             metadata= None
        01                                 data= length= 1 (int)
        00                                         body= Array: 0 items
        ---   server -> client:   ---
        0500 0000 0000 0000    length= 5 (64-bit LE)
        02                       body= Response_v1
        10                               id= 16 (int)
        00                             data= Ok
        01                                   length= 1 (int)
        00                                     body= Array: 0 items
        |}];
      let%bind () = dispatch ~client in
      print_payload_messages_bidirectional ~s_to_c ~c_to_s;
      [%expect
        {|
        ---   client -> server:   ---
        0b00 0000 0000 0000    length= 11 (64-bit LE)
        03                       body= Query_v2
        0473 6f72 74                        tag= sort (4 bytes)
        01                              version= 1 (int)
        11                                   id= 17 (int)
        00                             metadata= None
        01                                 data= length= 1 (int)
        00                                         body= Array: 0 items
        ---   server -> client:   ---
        0500 0000 0000 0000    length= 5 (64-bit LE)
        02                       body= Response_v1
        11                               id= 17 (int)
        00                             data= Ok
        01                                   length= 1 (int)
        00                                     body= Array: 0 items
        |}];
      return ())
;;

(* Compatibility tests *)

let%expect_test "[V1 -> V2] RPC connection" =
  Test_helpers.with_rpc_server_connection
    ()
    ~client_header:Test_helpers.Header.v1
    ~server_header:Test_helpers.Header.v2
    ~f:(fun ~client ~server:_ ~s_to_c ~c_to_s ->
      print_headers ~s_to_c ~c_to_s;
      [%expect
        {|
        ---   client -> server:   ---
        0700 0000 0000 0000    length= 7 (64-bit LE)
        02                       body= List: 2 items
        fd52 5043 00                   0: 4411474 (int)
        01                             1: 1 (int)
        ---   server -> client:   ---
        0800 0000 0000 0000    length= 8 (64-bit LE)
        03                       body= List: 3 items
        fd52 5043 00                   0: 4411474 (int)
        01                             1: 1 (int)
        02                             2: 2 (int)
        |}];
      let%bind () = dispatch ~client in
      print_payload_messages_bidirectional ~s_to_c ~c_to_s;
      [%expect
        {|
        ---   client -> server:   ---
        1400 0000 0000 0000    length= 20 (64-bit LE)
        01                       body= Query_v1
        0473 6f72 74                       tag= sort (4 bytes)
        01                             version= 1 (int)
        12                                  id= 18 (int)
        0b                                data= length= 11 (int)
        04                                        body= Array: 4 items
        fe80 01                                         0: 384 (int)
        fee5 03                                         1: 997 (int)
        fea6 00                                         2: 166 (int)
        23                                              3: 35 (int)

        0100 0000 0000 0000    length= 1 (64-bit LE)
        00                       body= Heartbeat
        ---   server -> client:   ---
        0100 0000 0000 0000    length= 1 (64-bit LE)
        00                       body= Heartbeat

        0f00 0000 0000 0000    length= 15 (64-bit LE)
        02                       body= Response_v1
        12                               id= 18 (int)
        00                             data= Ok
        0b                                   length= 11 (int)
        04                                     body= Array: 4 items
        23                                           0: 35 (int)
        fea6 00                                      1: 166 (int)
        fe80 01                                      2: 384 (int)
        fee5 03                                      3: 997 (int)
        |}];
      let%bind () = dispatch ~client in
      print_payload_messages_bidirectional ~s_to_c ~c_to_s;
      [%expect
        {|
        ---   client -> server:   ---
        0a00 0000 0000 0000    length= 10 (64-bit LE)
        01                       body= Query_v1
        0473 6f72 74                       tag= sort (4 bytes)
        01                             version= 1 (int)
        13                                  id= 19 (int)
        01                                data= length= 1 (int)
        00                                        body= Array: 0 items
        ---   server -> client:   ---
        0500 0000 0000 0000    length= 5 (64-bit LE)
        02                       body= Response_v1
        13                               id= 19 (int)
        00                             data= Ok
        01                                   length= 1 (int)
        00                                     body= Array: 0 items
        |}];
      let%bind _ = dispatch ~client in
      print_payload_messages_bidirectional ~s_to_c ~c_to_s;
      [%expect
        {|
        ---   client -> server:   ---
        0a00 0000 0000 0000    length= 10 (64-bit LE)
        01                       body= Query_v1
        0473 6f72 74                       tag= sort (4 bytes)
        01                             version= 1 (int)
        14                                  id= 20 (int)
        01                                data= length= 1 (int)
        00                                        body= Array: 0 items
        ---   server -> client:   ---
        0500 0000 0000 0000    length= 5 (64-bit LE)
        02                       body= Response_v1
        14                               id= 20 (int)
        00                             data= Ok
        01                                   length= 1 (int)
        00                                     body= Array: 0 items
        |}];
      return ())
;;

let%expect_test "[V2 -> V1] RPC connection" =
  Test_helpers.with_rpc_server_connection
    ()
    ~client_header:Test_helpers.Header.v2
    ~server_header:Test_helpers.Header.v1
    ~f:(fun ~client ~server:_ ~s_to_c ~c_to_s ->
      print_headers ~s_to_c ~c_to_s;
      [%expect
        {|
        ---   client -> server:   ---
        0800 0000 0000 0000    length= 8 (64-bit LE)
        03                       body= List: 3 items
        fd52 5043 00                   0: 4411474 (int)
        01                             1: 1 (int)
        02                             2: 2 (int)
        ---   server -> client:   ---
        0700 0000 0000 0000    length= 7 (64-bit LE)
        02                       body= List: 2 items
        fd52 5043 00                   0: 4411474 (int)
        01                             1: 1 (int)
        |}];
      let%bind () = dispatch ~client in
      print_payload_messages_bidirectional ~s_to_c ~c_to_s;
      [%expect
        {|
        ---   client -> server:   ---
        1400 0000 0000 0000    length= 20 (64-bit LE)
        01                       body= Query_v1
        0473 6f72 74                       tag= sort (4 bytes)
        01                             version= 1 (int)
        15                                  id= 21 (int)
        0b                                data= length= 11 (int)
        04                                        body= Array: 4 items
        fe80 01                                         0: 384 (int)
        fee5 03                                         1: 997 (int)
        fea6 00                                         2: 166 (int)
        23                                              3: 35 (int)

        0100 0000 0000 0000    length= 1 (64-bit LE)
        00                       body= Heartbeat
        ---   server -> client:   ---
        0100 0000 0000 0000    length= 1 (64-bit LE)
        00                       body= Heartbeat

        0f00 0000 0000 0000    length= 15 (64-bit LE)
        02                       body= Response_v1
        15                               id= 21 (int)
        00                             data= Ok
        0b                                   length= 11 (int)
        04                                     body= Array: 4 items
        23                                           0: 35 (int)
        fea6 00                                      1: 166 (int)
        fe80 01                                      2: 384 (int)
        fee5 03                                      3: 997 (int)
        |}];
      let%bind () = dispatch ~client in
      print_payload_messages_bidirectional ~s_to_c ~c_to_s;
      [%expect
        {|
        ---   client -> server:   ---
        0a00 0000 0000 0000    length= 10 (64-bit LE)
        01                       body= Query_v1
        0473 6f72 74                       tag= sort (4 bytes)
        01                             version= 1 (int)
        16                                  id= 22 (int)
        01                                data= length= 1 (int)
        00                                        body= Array: 0 items
        ---   server -> client:   ---
        0500 0000 0000 0000    length= 5 (64-bit LE)
        02                       body= Response_v1
        16                               id= 22 (int)
        00                             data= Ok
        01                                   length= 1 (int)
        00                                     body= Array: 0 items
        |}];
      let%bind () = dispatch ~client in
      print_payload_messages_bidirectional ~s_to_c ~c_to_s;
      [%expect
        {|
        ---   client -> server:   ---
        0a00 0000 0000 0000    length= 10 (64-bit LE)
        01                       body= Query_v1
        0473 6f72 74                       tag= sort (4 bytes)
        01                             version= 1 (int)
        17                                  id= 23 (int)
        01                                data= length= 1 (int)
        00                                        body= Array: 0 items
        ---   server -> client:   ---
        0500 0000 0000 0000    length= 5 (64-bit LE)
        02                       body= Response_v1
        17                               id= 23 (int)
        00                             data= Ok
        01                                   length= 1 (int)
        00                                     body= Array: 0 items
        |}];
      return ())
;;

let%expect_test "expert v2 dispatch" =
  Test_helpers.with_circular_connection
    ~header:Test_helpers.Header.v2
    ~f:(fun conn tap ->
      print_header tap;
      [%expect
        {|
        0800 0000 0000 0000    length= 8 (64-bit LE)
        03                       body= List: 3 items
        fd52 5043 00                   0: 4411474 (int)
        01                             1: 1 (int)
        02                             2: 2 (int)
        |}];
      let payload = Payload.create () in
      let%bind _response = dispatch_expert ~client:conn ~payload in
      print_payload_messages tap;
      [%expect
        {|
        1500 0000 0000 0000    length= 21 (64-bit LE)
        03                       body= Query_v2
        0473 6f72 74                        tag= sort (4 bytes)
        01                              version= 1 (int)
        18                                   id= 24 (int)
        00                             metadata= None
        0b                                 data= length= 11 (int)
        04                                         body= Array: 4 items
        fe80 01                                          0: 384 (int)
        fee5 03                                          1: 997 (int)
        fea6 00                                          2: 166 (int)
        23                                               3: 35 (int)

        0100 0000 0000 0000    length= 1 (64-bit LE)
        00                       body= Heartbeat

        0f00 0000 0000 0000    length= 15 (64-bit LE)
        02                       body= Response_v1
        18                               id= 24 (int)
        00                             data= Ok
        0b                                   length= 11 (int)
        04                                     body= Array: 4 items
        23                                           0: 35 (int)
        fea6 00                                      1: 166 (int)
        fe80 01                                      2: 384 (int)
        fee5 03                                      3: 997 (int)
        |}];
      let%bind _response = dispatch_expert ~client:conn ~payload in
      print_payload_messages tap;
      [%expect
        {|
        1500 0000 0000 0000    length= 21 (64-bit LE)
        03                       body= Query_v2
        0473 6f72 74                        tag= sort (4 bytes)
        01                              version= 1 (int)
        19                                   id= 25 (int)
        00                             metadata= None
        0b                                 data= length= 11 (int)
        04                                         body= Array: 4 items
        fe80 01                                          0: 384 (int)
        fee5 03                                          1: 997 (int)
        fea6 00                                          2: 166 (int)
        23                                               3: 35 (int)

        0f00 0000 0000 0000    length= 15 (64-bit LE)
        02                       body= Response_v1
        19                               id= 25 (int)
        00                             data= Ok
        0b                                   length= 11 (int)
        04                                     body= Array: 4 items
        23                                           0: 35 (int)
        fea6 00                                      1: 166 (int)
        fe80 01                                      2: 384 (int)
        fee5 03                                      3: 997 (int)
        |}];
      let%bind _response = dispatch_expert ~client:conn ~payload in
      print_payload_messages tap;
      [%expect
        {|
        1500 0000 0000 0000    length= 21 (64-bit LE)
        03                       body= Query_v2
        0473 6f72 74                        tag= sort (4 bytes)
        01                              version= 1 (int)
        1a                                   id= 26 (int)
        00                             metadata= None
        0b                                 data= length= 11 (int)
        04                                         body= Array: 4 items
        fe80 01                                          0: 384 (int)
        fee5 03                                          1: 997 (int)
        fea6 00                                          2: 166 (int)
        23                                               3: 35 (int)

        0f00 0000 0000 0000    length= 15 (64-bit LE)
        02                       body= Response_v1
        1a                               id= 26 (int)
        00                             data= Ok
        0b                                   length= 11 (int)
        04                                     body= Array: 4 items
        23                                           0: 35 (int)
        fea6 00                                      1: 166 (int)
        fe80 01                                      2: 384 (int)
        fee5 03                                      3: 997 (int)
        |}];
      return ())
    ()
;;

let%expect_test "expert v1 dispatch" =
  Test_helpers.with_circular_connection
    ~header:Test_helpers.Header.v1
    ~f:(fun conn tap ->
      print_header tap;
      [%expect
        {|
        0700 0000 0000 0000    length= 7 (64-bit LE)
        02                       body= List: 2 items
        fd52 5043 00                   0: 4411474 (int)
        01                             1: 1 (int)
        |}];
      let payload = Payload.create () in
      let%bind (_ : unit) = dispatch_expert ~client:conn ~payload in
      print_payload_messages tap;
      [%expect
        {|
        1400 0000 0000 0000    length= 20 (64-bit LE)
        01                       body= Query_v1
        0473 6f72 74                       tag= sort (4 bytes)
        01                             version= 1 (int)
        1b                                  id= 27 (int)
        0b                                data= length= 11 (int)
        04                                        body= Array: 4 items
        fe80 01                                         0: 384 (int)
        fee5 03                                         1: 997 (int)
        fea6 00                                         2: 166 (int)
        23                                              3: 35 (int)

        0100 0000 0000 0000    length= 1 (64-bit LE)
        00                       body= Heartbeat

        0f00 0000 0000 0000    length= 15 (64-bit LE)
        02                       body= Response_v1
        1b                               id= 27 (int)
        00                             data= Ok
        0b                                   length= 11 (int)
        04                                     body= Array: 4 items
        23                                           0: 35 (int)
        fea6 00                                      1: 166 (int)
        fe80 01                                      2: 384 (int)
        fee5 03                                      3: 997 (int)
        |}];
      let%bind (_ : unit) = dispatch_expert ~client:conn ~payload in
      print_payload_messages tap;
      [%expect
        {|
        1400 0000 0000 0000    length= 20 (64-bit LE)
        01                       body= Query_v1
        0473 6f72 74                       tag= sort (4 bytes)
        01                             version= 1 (int)
        1c                                  id= 28 (int)
        0b                                data= length= 11 (int)
        04                                        body= Array: 4 items
        fe80 01                                         0: 384 (int)
        fee5 03                                         1: 997 (int)
        fea6 00                                         2: 166 (int)
        23                                              3: 35 (int)

        0f00 0000 0000 0000    length= 15 (64-bit LE)
        02                       body= Response_v1
        1c                               id= 28 (int)
        00                             data= Ok
        0b                                   length= 11 (int)
        04                                     body= Array: 4 items
        23                                           0: 35 (int)
        fea6 00                                      1: 166 (int)
        fe80 01                                      2: 384 (int)
        fee5 03                                      3: 997 (int)
        |}];
      let%bind (_ : unit) = dispatch_expert ~client:conn ~payload in
      print_payload_messages tap;
      [%expect
        {|
        1400 0000 0000 0000    length= 20 (64-bit LE)
        01                       body= Query_v1
        0473 6f72 74                       tag= sort (4 bytes)
        01                             version= 1 (int)
        1d                                  id= 29 (int)
        0b                                data= length= 11 (int)
        04                                        body= Array: 4 items
        fe80 01                                         0: 384 (int)
        fee5 03                                         1: 997 (int)
        fea6 00                                         2: 166 (int)
        23                                              3: 35 (int)

        0f00 0000 0000 0000    length= 15 (64-bit LE)
        02                       body= Response_v1
        1d                               id= 29 (int)
        00                             data= Ok
        0b                                   length= 11 (int)
        04                                     body= Array: 4 items
        23                                           0: 35 (int)
        fea6 00                                      1: 166 (int)
        fe80 01                                      2: 384 (int)
        fee5 03                                      3: 997 (int)
        |}];
      return ())
    ()
;;

let%expect_test "regression test: closing a connection before [connection_state] returns \
                 shouldn't leak the connection"
  =
  let empty_implementations =
    Rpc.Implementations.create_exn
      ~implementations:[]
      ~on_unknown_rpc:`Raise
      ~on_exception:Log_on_background_exn
  in
  let%bind server =
    Rpc.Connection.serve
      ~implementations:empty_implementations
      ~initial_connection_state:(fun _ (_ : Rpc.Connection.t) -> ())
      ~where_to_listen:Tcp.Where_to_listen.of_port_chosen_by_os
      ()
  in
  let connection_weak_pointer = Weak_pointer.create () in
  (* We try to ensure that [connection] doesn't leave the scope of this function except
     for [connection_weak_pointer], so that we can ensure it gets GC'ed with a Gc.major. *)
  let connect_and_set_weak_pointer ~when_to_close_connection =
    let connection_state connection =
      let close () = Rpc.Connection.close connection |> don't_wait_for in
      match when_to_close_connection with
      | `Within_connection_state -> close ()
      | `After_connection_state -> upon (return ()) close
    in
    let%bind connection =
      Rpc.Connection.client
        ~implementations:(T { connection_state; implementations = empty_implementations })
        (Tcp.Where_to_connect.of_host_and_port
           { host = "localhost"; port = Tcp.Server.listening_on server })
      >>| Result.ok_exn
    in
    Weak_pointer.set connection_weak_pointer (Heap_block.create_exn connection);
    Rpc.Connection.close_finished connection
  in
  let test ~when_to_close_connection =
    let%bind () = connect_and_set_weak_pointer ~when_to_close_connection in
    let%map () = Scheduler.yield_until_no_jobs_remain () in
    Gc.full_major ();
    print_endline
      [%string
        "Connection object still held live: %{Weak_pointer.is_some \
         connection_weak_pointer#Bool}"]
  in
  let%bind () = test ~when_to_close_connection:`After_connection_state in
  [%expect {| Connection object still held live: false |}];
  let%bind () = test ~when_to_close_connection:`Within_connection_state in
  [%expect {| Connection object still held live: false |}];
  return ()
;;

let%expect_test "regression test: writer closing between handshake and metadata being \
                 sent does not cause an exception to be thrown"
  =
  let time_source =
    Synchronous_time_source.create ~now:Time_ns.epoch ()
    |> Synchronous_time_source.read_only
  in
  let transport_a, transport_b =
    Async_rpc_kernel.Pipe_transport.(create_pair Kind.string)
  in
  let conn_a =
    Async_rpc_kernel.Async_rpc_kernel_private.Connection.create
      ~time_source
      ~connection_state:(fun (_ : Rpc.Connection.t) -> ())
      transport_a
  in
  let conn_b =
    Async_rpc_kernel.Async_rpc_kernel_private.Connection.create
      ~time_source
      ~connection_state:(fun (_ : Rpc.Connection.t) -> ())
      transport_b
  in
  (* At this point, conn_a and conn_b will have synchronously written the headers to the
     pipe, and are binding on seeing the message from the other. If we close conn_a's
     writer here, then both will successfully negotiate, but conn_a will fail to write
     metadata and result in an error *)
  don't_wait_for (Rpc.Transport.Writer.close transport_a.writer);
  let wait_and_print_result conn =
    let%map conn in
    print_s [%sexp (conn : (_, exn) Result.t)]
  in
  let%bind () = wait_and_print_result conn_a in
  [%expect
    {|
    (Error
     (handshake_error.ml.Handshake_error
      ((Transport_closed_during_step Connection_metadata) <created-directly>)))
    |}];
  let%bind () = wait_and_print_result conn_b in
  [%expect
    {|
    (Error
     (handshake_error.ml.Handshake_error
      ((Transport_closed_during_step Connection_metadata) <created-directly>)))
    |}];
  return ()
;;

let%expect_test "V10 Close_reason_v2 addition" =
  let connection_test_with_close ~client ~server ~s_to_c ~c_to_s =
    print_headers ~s_to_c ~c_to_s;
    let%bind server_id_from_client = Rpc.Connection.peer_identification client in
    let%bind client_id_from_server = Rpc.Connection.peer_identification server in
    let%map () =
      Rpc.Connection.close client ~reason:(Info.create_s [%message "test reason"])
    in
    print_payload_messages_bidirectional ~s_to_c ~c_to_s;
    print_s
      [%message
        (server_id_from_client : Bigstring.t option)
          (client_id_from_server : Bigstring.t option)]
  in
  let%bind () =
    Test_helpers.with_rpc_server_connection
      ()
      ~server_header:Test_helpers.Header.v10
      ~client_header:Test_helpers.Header.v10
      ~f:connection_test_with_close
  in
  [%expect
    {|
    ---   client -> server:   ---
    1000 0000 0000 0000    length= 16 (64-bit LE)
    0b                       body= List: 11 items
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

    0d00 0000 0000 0000    length= 13 (64-bit LE)
    07                       body= Metadata_v2
    01                             identification= Some
    0763 6c69 6e2d 6964                             clin-id (7 bytes)
    01                                       menu= Some
    00                                              descriptions= Array: 0 items
    00                                                   digests= None
    ---   server -> client:   ---
    1000 0000 0000 0000    length= 16 (64-bit LE)
    0b                       body= List: 11 items
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

    5800 0000 0000 0000    length= 88 (64-bit LE)
    07                       body= Metadata_v2
    01                             identification= Some
    0773 6572 762d 6964                             serv-id (7 bytes)
    01                                       menu= Some
    06                                              descriptions= Array: 6 items
    0473 6f72 74                                                  0:    name= sort (4 bytes)
    01                                                               version= 1 (int)
    1074 6573 742d 6f6e    ...
    652d 7761 792d 7270    ...
    63                                                            1:    name= test-on... (16 bytes)
    01                                                               version= 1 (int)
    0d74 6573 742d 7069    ...
    7065 2d72 7063                                                2:    name= test-pi... (13 bytes)
    01                                                               version= 1 (int)
    0874 6573 742d 7270    ...
    63                                                            3:    name= test-rpc (8 bytes)
    01                                                               version= 1 (int)
    0874 6573 742d 7270    ...
    63                                                            4:    name= test-rpc (8 bytes)
    02                                                               version= 2 (int)
    0e74 6573 742d 7374    ...
    6174 652d 7270 63                                             5:    name= test-st... (14 bytes)
    01                                                               version= 1 (int)
    00                                                   digests= None
    ---   client -> server:   ---
    1e00 0000 0000 0000    length= 30 (64-bit LE)
    0b                       body= Close_reason_v2
    00                                    kind=   Atom
    0b55 6e73 7065 6369    ...
    6669 6564                                     Unspeci... (11 bytes)
    00                              debug_info= None
    01                             user_reason= Some
    03                                             Sexp
    00                                               Atom
    0b74 6573 7420 7265    ...
    6173 6f6e                                        "test re..." (11 bytes)
    ---   server -> client:   ---
    0100 0000 0000 0000    length= 1 (64-bit LE)
    00                       body= Heartbeat
    ((server_id_from_client (serv-id)) (client_id_from_server (clin-id)))
    |}];
  let%bind () =
    Test_helpers.with_rpc_server_connection
      ()
      ~server_header:Test_helpers.Header.v10
      ~client_header:Test_helpers.Header.v9
      ~f:connection_test_with_close
  in
  [%expect
    {|
    ---   client -> server:   ---
    0f00 0000 0000 0000    length= 15 (64-bit LE)
    0a                       body= List: 10 items
    fd52 5043 00                   0: 4411474 (int)
    01                             1: 1 (int)
    02                             2: 2 (int)
    03                             3: 3 (int)
    04                             4: 4 (int)
    05                             5: 5 (int)
    06                             6: 6 (int)
    07                             7: 7 (int)
    08                             8: 8 (int)
    09                             9: 9 (int)

    0d00 0000 0000 0000    length= 13 (64-bit LE)
    07                       body= Metadata_v2
    01                             identification= Some
    0763 6c69 6e2d 6964                             clin-id (7 bytes)
    01                                       menu= Some
    00                                              descriptions= Array: 0 items
    00                                                   digests= None
    ---   server -> client:   ---
    1000 0000 0000 0000    length= 16 (64-bit LE)
    0b                       body= List: 11 items
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

    5800 0000 0000 0000    length= 88 (64-bit LE)
    07                       body= Metadata_v2
    01                             identification= Some
    0773 6572 762d 6964                             serv-id (7 bytes)
    01                                       menu= Some
    06                                              descriptions= Array: 6 items
    0473 6f72 74                                                  0:    name= sort (4 bytes)
    01                                                               version= 1 (int)
    1074 6573 742d 6f6e    ...
    652d 7761 792d 7270    ...
    63                                                            1:    name= test-on... (16 bytes)
    01                                                               version= 1 (int)
    0d74 6573 742d 7069    ...
    7065 2d72 7063                                                2:    name= test-pi... (13 bytes)
    01                                                               version= 1 (int)
    0874 6573 742d 7270    ...
    63                                                            3:    name= test-rpc (8 bytes)
    01                                                               version= 1 (int)
    0874 6573 742d 7270    ...
    63                                                            4:    name= test-rpc (8 bytes)
    02                                                               version= 2 (int)
    0e74 6573 742d 7374    ...
    6174 652d 7270 63                                             5:    name= test-st... (14 bytes)
    01                                                               version= 1 (int)
    00                                                   digests= None
    ---   client -> server:   ---
    0f00 0000 0000 0000    length= 15 (64-bit LE)
    05                       body= Close_reason
    03                               Sexp
    00                                 Atom
    0b74 6573 7420 7265    ...
    6173 6f6e                          "test re..." (11 bytes)
    ---   server -> client:   ---
    0100 0000 0000 0000    length= 1 (64-bit LE)
    00                       body= Heartbeat
    ((server_id_from_client (serv-id)) (client_id_from_server (clin-id)))
    |}];
  let%bind () =
    Test_helpers.with_rpc_server_connection
      ()
      ~server_header:Test_helpers.Header.v9
      ~client_header:Test_helpers.Header.v10
      ~f:connection_test_with_close
  in
  [%expect
    {|
    ---   client -> server:   ---
    1000 0000 0000 0000    length= 16 (64-bit LE)
    0b                       body= List: 11 items
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

    0d00 0000 0000 0000    length= 13 (64-bit LE)
    07                       body= Metadata_v2
    01                             identification= Some
    0763 6c69 6e2d 6964                             clin-id (7 bytes)
    01                                       menu= Some
    00                                              descriptions= Array: 0 items
    00                                                   digests= None
    ---   server -> client:   ---
    0f00 0000 0000 0000    length= 15 (64-bit LE)
    0a                       body= List: 10 items
    fd52 5043 00                   0: 4411474 (int)
    01                             1: 1 (int)
    02                             2: 2 (int)
    03                             3: 3 (int)
    04                             4: 4 (int)
    05                             5: 5 (int)
    06                             6: 6 (int)
    07                             7: 7 (int)
    08                             8: 8 (int)
    09                             9: 9 (int)

    5800 0000 0000 0000    length= 88 (64-bit LE)
    07                       body= Metadata_v2
    01                             identification= Some
    0773 6572 762d 6964                             serv-id (7 bytes)
    01                                       menu= Some
    06                                              descriptions= Array: 6 items
    0473 6f72 74                                                  0:    name= sort (4 bytes)
    01                                                               version= 1 (int)
    1074 6573 742d 6f6e    ...
    652d 7761 792d 7270    ...
    63                                                            1:    name= test-on... (16 bytes)
    01                                                               version= 1 (int)
    0d74 6573 742d 7069    ...
    7065 2d72 7063                                                2:    name= test-pi... (13 bytes)
    01                                                               version= 1 (int)
    0874 6573 742d 7270    ...
    63                                                            3:    name= test-rpc (8 bytes)
    01                                                               version= 1 (int)
    0874 6573 742d 7270    ...
    63                                                            4:    name= test-rpc (8 bytes)
    02                                                               version= 2 (int)
    0e74 6573 742d 7374    ...
    6174 652d 7270 63                                             5:    name= test-st... (14 bytes)
    01                                                               version= 1 (int)
    00                                                   digests= None
    ---   client -> server:   ---
    0f00 0000 0000 0000    length= 15 (64-bit LE)
    05                       body= Close_reason
    03                               Sexp
    00                                 Atom
    0b74 6573 7420 7265    ...
    6173 6f6e                          "test re..." (11 bytes)
    ---   server -> client:   ---
    0100 0000 0000 0000    length= 1 (64-bit LE)
    00                       body= Heartbeat
    ((server_id_from_client (serv-id)) (client_id_from_server (clin-id)))
    |}];
  return ()
;;

let%expect_test "[V7 -> V8] Query_v3 compatibility test" =
  Test_helpers.with_rpc_server_connection
    ()
    ~client_header:Test_helpers.Header.v7
    ~server_header:Test_helpers.Header.v8
    ~f:(fun ~client ~server:_ ~s_to_c ~c_to_s ->
      print_headers ~s_to_c ~c_to_s;
      [%expect
        {|
        ---   client -> server:   ---
        0d00 0000 0000 0000    length= 13 (64-bit LE)
        08                       body= List: 8 items
        fd52 5043 00                   0: 4411474 (int)
        01                             1: 1 (int)
        02                             2: 2 (int)
        03                             3: 3 (int)
        04                             4: 4 (int)
        05                             5: 5 (int)
        06                             6: 6 (int)
        07                             7: 7 (int)

        0d00 0000 0000 0000    length= 13 (64-bit LE)
        07                       body= Metadata_v2
        01                             identification= Some
        0763 6c69 6e2d 6964                             clin-id (7 bytes)
        01                                       menu= Some
        00                                              descriptions= Array: 0 items
        00                                                   digests= None
        ---   server -> client:   ---
        0e00 0000 0000 0000    length= 14 (64-bit LE)
        09                       body= List: 9 items
        fd52 5043 00                   0: 4411474 (int)
        01                             1: 1 (int)
        02                             2: 2 (int)
        03                             3: 3 (int)
        04                             4: 4 (int)
        05                             5: 5 (int)
        06                             6: 6 (int)
        07                             7: 7 (int)
        08                             8: 8 (int)

        5800 0000 0000 0000    length= 88 (64-bit LE)
        07                       body= Metadata_v2
        01                             identification= Some
        0773 6572 762d 6964                             serv-id (7 bytes)
        01                                       menu= Some
        06                                              descriptions= Array: 6 items
        0473 6f72 74                                                  0:    name= sort (4 bytes)
        01                                                               version= 1 (int)
        1074 6573 742d 6f6e    ...
        652d 7761 792d 7270    ...
        63                                                            1:    name= test-on... (16 bytes)
        01                                                               version= 1 (int)
        0d74 6573 742d 7069    ...
        7065 2d72 7063                                                2:    name= test-pi... (13 bytes)
        01                                                               version= 1 (int)
        0874 6573 742d 7270    ...
        63                                                            3:    name= test-rpc (8 bytes)
        01                                                               version= 1 (int)
        0874 6573 742d 7270    ...
        63                                                            4:    name= test-rpc (8 bytes)
        02                                                               version= 2 (int)
        0e74 6573 742d 7374    ...
        6174 652d 7270 63                                             5:    name= test-st... (14 bytes)
        01                                                               version= 1 (int)
        00                                                   digests= None
        |}];
      let%bind dispatch_result =
        Rpc.Rpc.dispatch Test_helpers.sort_rpc client [| 5; 4; 3; 2; 1 |]
      in
      print_s [%message (dispatch_result : int array Or_error.t)];
      [%expect {| (dispatch_result (Ok (1 2 3 4 5))) |}];
      print_payload_messages_bidirectional ~s_to_c ~c_to_s;
      [%expect
        {|
        ---   client -> server:   ---
        1000 0000 0000 0000    length= 16 (64-bit LE)
        03                       body= Query_v2
        0473 6f72 74                        tag= sort (4 bytes)
        01                              version= 1 (int)
        1e                                   id= 30 (int)
        00                             metadata= None
        06                                 data= length= 6 (int)
        05                                         body= Array: 5 items
        05                                               0: 5 (int)
        04                                               1: 4 (int)
        03                                               2: 3 (int)
        02                                               3: 2 (int)
        01                                               4: 1 (int)

        0100 0000 0000 0000    length= 1 (64-bit LE)
        00                       body= Heartbeat
        ---   server -> client:   ---
        0100 0000 0000 0000    length= 1 (64-bit LE)
        00                       body= Heartbeat

        0c00 0000 0000 0000    length= 12 (64-bit LE)
        08                       body= Response_v2
        1e                                          id= 30 (int)
        01                             impl_menu_index= Some
        00                                               0 (nat0)
        00                                        data= Ok
        06                                              length= 6 (int)
        05                                                body= Array: 5 items
        01                                                      0: 1 (int)
        02                                                      1: 2 (int)
        03                                                      2: 3 (int)
        04                                                      3: 4 (int)
        05                                                      4: 5 (int)
        |}];
      return ())
;;

let%expect_test "[V8 -> V7] Query_v3 compatibility test" =
  Test_helpers.with_rpc_server_connection
    ()
    ~client_header:Test_helpers.Header.v8
    ~server_header:Test_helpers.Header.v7
    ~f:(fun ~client ~server:_ ~s_to_c ~c_to_s ->
      print_headers ~s_to_c ~c_to_s;
      [%expect
        {|
        ---   client -> server:   ---
        0e00 0000 0000 0000    length= 14 (64-bit LE)
        09                       body= List: 9 items
        fd52 5043 00                   0: 4411474 (int)
        01                             1: 1 (int)
        02                             2: 2 (int)
        03                             3: 3 (int)
        04                             4: 4 (int)
        05                             5: 5 (int)
        06                             6: 6 (int)
        07                             7: 7 (int)
        08                             8: 8 (int)

        0d00 0000 0000 0000    length= 13 (64-bit LE)
        07                       body= Metadata_v2
        01                             identification= Some
        0763 6c69 6e2d 6964                             clin-id (7 bytes)
        01                                       menu= Some
        00                                              descriptions= Array: 0 items
        00                                                   digests= None
        ---   server -> client:   ---
        0d00 0000 0000 0000    length= 13 (64-bit LE)
        08                       body= List: 8 items
        fd52 5043 00                   0: 4411474 (int)
        01                             1: 1 (int)
        02                             2: 2 (int)
        03                             3: 3 (int)
        04                             4: 4 (int)
        05                             5: 5 (int)
        06                             6: 6 (int)
        07                             7: 7 (int)

        5800 0000 0000 0000    length= 88 (64-bit LE)
        07                       body= Metadata_v2
        01                             identification= Some
        0773 6572 762d 6964                             serv-id (7 bytes)
        01                                       menu= Some
        06                                              descriptions= Array: 6 items
        0473 6f72 74                                                  0:    name= sort (4 bytes)
        01                                                               version= 1 (int)
        1074 6573 742d 6f6e    ...
        652d 7761 792d 7270    ...
        63                                                            1:    name= test-on... (16 bytes)
        01                                                               version= 1 (int)
        0d74 6573 742d 7069    ...
        7065 2d72 7063                                                2:    name= test-pi... (13 bytes)
        01                                                               version= 1 (int)
        0874 6573 742d 7270    ...
        63                                                            3:    name= test-rpc (8 bytes)
        01                                                               version= 1 (int)
        0874 6573 742d 7270    ...
        63                                                            4:    name= test-rpc (8 bytes)
        02                                                               version= 2 (int)
        0e74 6573 742d 7374    ...
        6174 652d 7270 63                                             5:    name= test-st... (14 bytes)
        01                                                               version= 1 (int)
        00                                                   digests= None
        |}];
      let%bind dispatch_result =
        Rpc.Rpc.dispatch Test_helpers.sort_rpc client [| 5; 4; 3; 2; 1 |]
      in
      print_s [%message (dispatch_result : int array Or_error.t)];
      [%expect {| (dispatch_result (Ok (1 2 3 4 5))) |}];
      print_payload_messages_bidirectional ~s_to_c ~c_to_s;
      [%expect
        {|
        ---   client -> server:   ---
        1000 0000 0000 0000    length= 16 (64-bit LE)
        03                       body= Query_v2
        0473 6f72 74                        tag= sort (4 bytes)
        01                              version= 1 (int)
        1f                                   id= 31 (int)
        00                             metadata= None
        06                                 data= length= 6 (int)
        05                                         body= Array: 5 items
        05                                               0: 5 (int)
        04                                               1: 4 (int)
        03                                               2: 3 (int)
        02                                               3: 2 (int)
        01                                               4: 1 (int)

        0100 0000 0000 0000    length= 1 (64-bit LE)
        00                       body= Heartbeat
        ---   server -> client:   ---
        0100 0000 0000 0000    length= 1 (64-bit LE)
        00                       body= Heartbeat

        0c00 0000 0000 0000    length= 12 (64-bit LE)
        08                       body= Response_v2
        1f                                          id= 31 (int)
        01                             impl_menu_index= Some
        00                                               0 (nat0)
        00                                        data= Ok
        06                                              length= 6 (int)
        05                                                body= Array: 5 items
        01                                                      0: 1 (int)
        02                                                      1: 2 (int)
        03                                                      2: 3 (int)
        04                                                      3: 4 (int)
        05                                                      4: 5 (int)
        |}];
      return ())
;;

let%expect_test "[V8 -> V8] Query_v3 same version test" =
  Test_helpers.with_rpc_server_connection
    ()
    ~client_header:Test_helpers.Header.v8
    ~server_header:Test_helpers.Header.v8
    ~f:(fun ~client ~server:_ ~s_to_c ~c_to_s ->
      print_headers ~s_to_c ~c_to_s;
      [%expect
        {|
        ---   client -> server:   ---
        0e00 0000 0000 0000    length= 14 (64-bit LE)
        09                       body= List: 9 items
        fd52 5043 00                   0: 4411474 (int)
        01                             1: 1 (int)
        02                             2: 2 (int)
        03                             3: 3 (int)
        04                             4: 4 (int)
        05                             5: 5 (int)
        06                             6: 6 (int)
        07                             7: 7 (int)
        08                             8: 8 (int)

        0d00 0000 0000 0000    length= 13 (64-bit LE)
        07                       body= Metadata_v2
        01                             identification= Some
        0763 6c69 6e2d 6964                             clin-id (7 bytes)
        01                                       menu= Some
        00                                              descriptions= Array: 0 items
        00                                                   digests= None
        ---   server -> client:   ---
        0e00 0000 0000 0000    length= 14 (64-bit LE)
        09                       body= List: 9 items
        fd52 5043 00                   0: 4411474 (int)
        01                             1: 1 (int)
        02                             2: 2 (int)
        03                             3: 3 (int)
        04                             4: 4 (int)
        05                             5: 5 (int)
        06                             6: 6 (int)
        07                             7: 7 (int)
        08                             8: 8 (int)

        5800 0000 0000 0000    length= 88 (64-bit LE)
        07                       body= Metadata_v2
        01                             identification= Some
        0773 6572 762d 6964                             serv-id (7 bytes)
        01                                       menu= Some
        06                                              descriptions= Array: 6 items
        0473 6f72 74                                                  0:    name= sort (4 bytes)
        01                                                               version= 1 (int)
        1074 6573 742d 6f6e    ...
        652d 7761 792d 7270    ...
        63                                                            1:    name= test-on... (16 bytes)
        01                                                               version= 1 (int)
        0d74 6573 742d 7069    ...
        7065 2d72 7063                                                2:    name= test-pi... (13 bytes)
        01                                                               version= 1 (int)
        0874 6573 742d 7270    ...
        63                                                            3:    name= test-rpc (8 bytes)
        01                                                               version= 1 (int)
        0874 6573 742d 7270    ...
        63                                                            4:    name= test-rpc (8 bytes)
        02                                                               version= 2 (int)
        0e74 6573 742d 7374    ...
        6174 652d 7270 63                                             5:    name= test-st... (14 bytes)
        01                                                               version= 1 (int)
        00                                                   digests= None
        |}];
      let%bind dispatch_result =
        Rpc.Rpc.dispatch Test_helpers.sort_rpc client [| 5; 4; 3; 2; 1 |]
      in
      print_s [%message (dispatch_result : int array Or_error.t)];
      [%expect {| (dispatch_result (Ok (1 2 3 4 5))) |}];
      print_payload_messages_bidirectional ~s_to_c ~c_to_s;
      [%expect
        {|
        ---   client -> server:   ---
        1000 0000 0000 0000    length= 16 (64-bit LE)
        09                       body= Query_v3
        0473 6f72 74                        tag= sort (4 bytes)
        01                              version= 1 (int)
        20                                   id= 32 (int)
        00                             metadata= None
        06                                 data= length= 6 (int)
        05                                         body= Array: 5 items
        05                                               0: 5 (int)
        04                                               1: 4 (int)
        03                                               2: 3 (int)
        02                                               3: 2 (int)
        01                                               4: 1 (int)

        0100 0000 0000 0000    length= 1 (64-bit LE)
        00                       body= Heartbeat
        ---   server -> client:   ---
        0100 0000 0000 0000    length= 1 (64-bit LE)
        00                       body= Heartbeat

        0c00 0000 0000 0000    length= 12 (64-bit LE)
        08                       body= Response_v2
        20                                          id= 32 (int)
        01                             impl_menu_index= Some
        00                                               0 (nat0)
        00                                        data= Ok
        06                                              length= 6 (int)
        05                                                body= Array: 5 items
        01                                                      0: 1 (int)
        02                                                      1: 2 (int)
        03                                                      2: 3 (int)
        04                                                      3: 4 (int)
        05                                                      4: 5 (int)
        |}];
      return ())
;;

let%expect_test "[V10 -> V11] Query_v4 compatibility test" =
  Test_helpers.with_rpc_server_connection
    ()
    ~client_header:Test_helpers.Header.v10
    ~server_header:Test_helpers.Header.v11
    ~f:(fun ~client ~server:_ ~s_to_c ~c_to_s ->
      print_headers ~s_to_c ~c_to_s;
      [%expect
        {|
        ---   client -> server:   ---
        1000 0000 0000 0000    length= 16 (64-bit LE)
        0b                       body= List: 11 items
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

        0d00 0000 0000 0000    length= 13 (64-bit LE)
        07                       body= Metadata_v2
        01                             identification= Some
        0763 6c69 6e2d 6964                             clin-id (7 bytes)
        01                                       menu= Some
        00                                              descriptions= Array: 0 items
        00                                                   digests= None
        ---   server -> client:   ---
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

        5800 0000 0000 0000    length= 88 (64-bit LE)
        07                       body= Metadata_v2
        01                             identification= Some
        0773 6572 762d 6964                             serv-id (7 bytes)
        01                                       menu= Some
        06                                              descriptions= Array: 6 items
        0473 6f72 74                                                  0:    name= sort (4 bytes)
        01                                                               version= 1 (int)
        1074 6573 742d 6f6e    ...
        652d 7761 792d 7270    ...
        63                                                            1:    name= test-on... (16 bytes)
        01                                                               version= 1 (int)
        0d74 6573 742d 7069    ...
        7065 2d72 7063                                                2:    name= test-pi... (13 bytes)
        01                                                               version= 1 (int)
        0874 6573 742d 7270    ...
        63                                                            3:    name= test-rpc (8 bytes)
        01                                                               version= 1 (int)
        0874 6573 742d 7270    ...
        63                                                            4:    name= test-rpc (8 bytes)
        02                                                               version= 2 (int)
        0e74 6573 742d 7374    ...
        6174 652d 7270 63                                             5:    name= test-st... (14 bytes)
        01                                                               version= 1 (int)
        00                                                   digests= None
        |}];
      let%bind dispatch_result =
        Rpc.Rpc.dispatch Test_helpers.sort_rpc client [| 5; 4; 3; 2; 1 |]
      in
      print_s [%message (dispatch_result : int array Or_error.t)];
      [%expect {| (dispatch_result (Ok (1 2 3 4 5))) |}];
      print_payload_messages_bidirectional ~s_to_c ~c_to_s;
      [%expect
        {|
        ---   client -> server:   ---
        1000 0000 0000 0000    length= 16 (64-bit LE)
        09                       body= Query_v3
        0473 6f72 74                        tag= sort (4 bytes)
        01                              version= 1 (int)
        21                                   id= 33 (int)
        00                             metadata= None
        06                                 data= length= 6 (int)
        05                                         body= Array: 5 items
        05                                               0: 5 (int)
        04                                               1: 4 (int)
        03                                               2: 3 (int)
        02                                               3: 2 (int)
        01                                               4: 1 (int)

        0100 0000 0000 0000    length= 1 (64-bit LE)
        00                       body= Heartbeat
        ---   server -> client:   ---
        0100 0000 0000 0000    length= 1 (64-bit LE)
        00                       body= Heartbeat

        0c00 0000 0000 0000    length= 12 (64-bit LE)
        08                       body= Response_v2
        21                                          id= 33 (int)
        01                             impl_menu_index= Some
        00                                               0 (nat0)
        00                                        data= Ok
        06                                              length= 6 (int)
        05                                                body= Array: 5 items
        01                                                      0: 1 (int)
        02                                                      1: 2 (int)
        03                                                      2: 3 (int)
        04                                                      3: 4 (int)
        05                                                      4: 5 (int)
        |}];
      return ())
;;

let%expect_test "[V11 -> V10] Query_v4 compatibility test" =
  Test_helpers.with_rpc_server_connection
    ()
    ~client_header:Test_helpers.Header.v11
    ~server_header:Test_helpers.Header.v10
    ~f:(fun ~client ~server:_ ~s_to_c ~c_to_s ->
      print_headers ~s_to_c ~c_to_s;
      [%expect
        {|
        ---   client -> server:   ---
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

        0d00 0000 0000 0000    length= 13 (64-bit LE)
        07                       body= Metadata_v2
        01                             identification= Some
        0763 6c69 6e2d 6964                             clin-id (7 bytes)
        01                                       menu= Some
        00                                              descriptions= Array: 0 items
        00                                                   digests= None
        ---   server -> client:   ---
        1000 0000 0000 0000    length= 16 (64-bit LE)
        0b                       body= List: 11 items
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

        5800 0000 0000 0000    length= 88 (64-bit LE)
        07                       body= Metadata_v2
        01                             identification= Some
        0773 6572 762d 6964                             serv-id (7 bytes)
        01                                       menu= Some
        06                                              descriptions= Array: 6 items
        0473 6f72 74                                                  0:    name= sort (4 bytes)
        01                                                               version= 1 (int)
        1074 6573 742d 6f6e    ...
        652d 7761 792d 7270    ...
        63                                                            1:    name= test-on... (16 bytes)
        01                                                               version= 1 (int)
        0d74 6573 742d 7069    ...
        7065 2d72 7063                                                2:    name= test-pi... (13 bytes)
        01                                                               version= 1 (int)
        0874 6573 742d 7270    ...
        63                                                            3:    name= test-rpc (8 bytes)
        01                                                               version= 1 (int)
        0874 6573 742d 7270    ...
        63                                                            4:    name= test-rpc (8 bytes)
        02                                                               version= 2 (int)
        0e74 6573 742d 7374    ...
        6174 652d 7270 63                                             5:    name= test-st... (14 bytes)
        01                                                               version= 1 (int)
        00                                                   digests= None
        |}];
      let%bind dispatch_result =
        Rpc.Rpc.dispatch Test_helpers.sort_rpc client [| 5; 4; 3; 2; 1 |]
      in
      print_s [%message (dispatch_result : int array Or_error.t)];
      [%expect {| (dispatch_result (Ok (1 2 3 4 5))) |}];
      print_payload_messages_bidirectional ~s_to_c ~c_to_s;
      [%expect
        {|
        ---   client -> server:   ---
        1000 0000 0000 0000    length= 16 (64-bit LE)
        09                       body= Query_v3
        0473 6f72 74                        tag= sort (4 bytes)
        01                              version= 1 (int)
        22                                   id= 34 (int)
        00                             metadata= None
        06                                 data= length= 6 (int)
        05                                         body= Array: 5 items
        05                                               0: 5 (int)
        04                                               1: 4 (int)
        03                                               2: 3 (int)
        02                                               3: 2 (int)
        01                                               4: 1 (int)

        0100 0000 0000 0000    length= 1 (64-bit LE)
        00                       body= Heartbeat
        ---   server -> client:   ---
        0100 0000 0000 0000    length= 1 (64-bit LE)
        00                       body= Heartbeat

        0c00 0000 0000 0000    length= 12 (64-bit LE)
        08                       body= Response_v2
        22                                          id= 34 (int)
        01                             impl_menu_index= Some
        00                                               0 (nat0)
        00                                        data= Ok
        06                                              length= 6 (int)
        05                                                body= Array: 5 items
        01                                                      0: 1 (int)
        02                                                      1: 2 (int)
        03                                                      2: 3 (int)
        04                                                      3: 4 (int)
        05                                                      4: 5 (int)
        |}];
      return ())
;;

let%expect_test "[V11 -> V11] Query_v4 same version test" =
  Test_helpers.with_rpc_server_connection
    ()
    ~client_header:Test_helpers.Header.v11
    ~server_header:Test_helpers.Header.v11
    ~f:(fun ~client ~server:_ ~s_to_c ~c_to_s ->
      print_headers ~s_to_c ~c_to_s;
      [%expect
        {|
        ---   client -> server:   ---
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

        0d00 0000 0000 0000    length= 13 (64-bit LE)
        07                       body= Metadata_v2
        01                             identification= Some
        0763 6c69 6e2d 6964                             clin-id (7 bytes)
        01                                       menu= Some
        00                                              descriptions= Array: 0 items
        00                                                   digests= None
        ---   server -> client:   ---
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

        5800 0000 0000 0000    length= 88 (64-bit LE)
        07                       body= Metadata_v2
        01                             identification= Some
        0773 6572 762d 6964                             serv-id (7 bytes)
        01                                       menu= Some
        06                                              descriptions= Array: 6 items
        0473 6f72 74                                                  0:    name= sort (4 bytes)
        01                                                               version= 1 (int)
        1074 6573 742d 6f6e    ...
        652d 7761 792d 7270    ...
        63                                                            1:    name= test-on... (16 bytes)
        01                                                               version= 1 (int)
        0d74 6573 742d 7069    ...
        7065 2d72 7063                                                2:    name= test-pi... (13 bytes)
        01                                                               version= 1 (int)
        0874 6573 742d 7270    ...
        63                                                            3:    name= test-rpc (8 bytes)
        01                                                               version= 1 (int)
        0874 6573 742d 7270    ...
        63                                                            4:    name= test-rpc (8 bytes)
        02                                                               version= 2 (int)
        0e74 6573 742d 7374    ...
        6174 652d 7270 63                                             5:    name= test-st... (14 bytes)
        01                                                               version= 1 (int)
        00                                                   digests= None
        |}];
      let%bind dispatch_result =
        Rpc.Rpc.dispatch Test_helpers.sort_rpc client [| 5; 4; 3; 2; 1 |]
      in
      print_s [%message (dispatch_result : int array Or_error.t)];
      [%expect {| (dispatch_result (Ok (1 2 3 4 5))) |}];
      print_payload_messages_bidirectional ~s_to_c ~c_to_s;
      [%expect
        {|
        ---   client -> server:   ---
        0c00 0000 0000 0000    length= 12 (64-bit LE)
        0c                       body= Query_v4
        01                             specifier= Rank
        00                                        0 (int)
        23                                    id= 35 (int)
        00                              metadata= None
        06                                  data= length= 6 (int)
        05                                          body= Array: 5 items
        05                                                0: 5 (int)
        04                                                1: 4 (int)
        03                                                2: 3 (int)
        02                                                3: 2 (int)
        01                                                4: 1 (int)

        0100 0000 0000 0000    length= 1 (64-bit LE)
        00                       body= Heartbeat
        ---   server -> client:   ---
        0100 0000 0000 0000    length= 1 (64-bit LE)
        00                       body= Heartbeat

        0c00 0000 0000 0000    length= 12 (64-bit LE)
        08                       body= Response_v2
        23                                          id= 35 (int)
        01                             impl_menu_index= Some
        00                                               0 (nat0)
        00                                        data= Ok
        06                                              length= 6 (int)
        05                                                body= Array: 5 items
        01                                                      0: 1 (int)
        02                                                      1: 2 (int)
        03                                                      2: 3 (int)
        04                                                      3: 4 (int)
        05                                                      4: 5 (int)
        |}];
      return ())
;;
