open! Core
open! Async

module Payload = struct
  type t = int array [@@deriving bin_io]

  let get_random_size () = Random.int 5

  let create () =
    let size = get_random_size () in
    size |> Array.init ~f:(fun (_ : int) -> Random.int 1000)
  ;;
end

let print_header = Test_helpers.Tap.print_header
let print_headers = Test_helpers.Tap.print_headers
let print_messages tap = Test_helpers.Tap.print_messages tap [%bin_shape: Payload.t]

let print_messages_bidirectional =
  Test_helpers.Tap.print_messages_bidirectional [%bin_shape: Payload.t]
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
      ~handle_error:(fun err ->
      failwiths ~here:[%here] "expert rpc error" err [%sexp_of: Error.t])
  with
  | `Connection_closed -> failwith "connection closed"
  | `Ok -> Ivar.read wait_for_response
;;

let connection_test_id ~client ~server ~s_to_c ~c_to_s =
  print_headers ~s_to_c ~c_to_s;
  let%bind server_id_from_client = Rpc.Connection.peer_identification client in
  let%map client_id_from_server = Rpc.Connection.peer_identification server in
  print_messages_bidirectional ~s_to_c ~c_to_s;
  print_s
    [%message
      (server_id_from_client : Bigstring.t option)
        (client_id_from_server : Bigstring.t option)]
;;

let connection_test_menu ~client ~server ~s_to_c:_ ~c_to_s:_ =
  let%bind client_menu = Rpc.Connection.peer_menu server in
  let%map server_menu = Rpc.Connection.peer_menu client in
  print_s
    [%message
      (client_menu : Async_rpc_kernel.Menu.With_digests_in_sexp.t option Or_error.t)];
  print_s
    [%message
      (server_menu : Async_rpc_kernel.Menu.With_digests_in_sexp.t option Or_error.t)]
;;

let connection_test ~server_header ~client_header ~f : unit Deferred.t =
  Test_helpers.with_rpc_server_connection
    ~server_header
    ~client_header
    ~f:(fun ~client ~server ~s_to_c ~c_to_s -> f ~client ~server ~s_to_c ~c_to_s)
;;

let%expect_test "V3 send versioned menu automatically" =
  let%bind () =
    connection_test
      ~server_header:Test_helpers.Header.v3
      ~client_header:Test_helpers.Header.v3
      ~f:connection_test_menu
  in
  [%expect
    {|
    (client_menu (Ok (())))
    (server_menu
     (Ok
      ((((name sort)
         (versions
          (((version 1)
            (digest
             (Rpc (query 4c138035aa69ec9dd8b7a7119090f84a)
              (response 4c138035aa69ec9dd8b7a7119090f84a)))))))
        ((name test-one-way-rpc)
         (versions
          (((version 1)
            (digest (One_way (msg e2d261c6c291b94bf6aa68ec2b08cb00)))))))
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
              (error 52966f4a49a77bfdff668e9cc61511b3)))))))))))
    |}];
  let%bind () =
    connection_test
      ~server_header:Test_helpers.Header.v3
      ~client_header:Test_helpers.Header.v2
      ~f:connection_test_menu
  in
  [%expect {|
    (client_menu (Ok ()))
    (server_menu (Ok ()))
    |}];
  return ()
;;

let%expect_test "V3 identification string addition" =
  let%bind () =
    connection_test
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
    ---   server -> client:   ---
    0900 0000 0000 0000    length= 9 (64-bit LE)
    04                       body= List: 4 items
    fd52 5043 00                   0: 4411474 (int)
    01                             1: 1 (int)
    02                             2: 2 (int)
    03                             3: 3 (int)
    ---   client -> server:   ---
    0c00 0000 0000 0000    length= 12 (64-bit LE)
    04                       body= Metadata
    01                             identification= Some
    0763 6c69 6e2d 6964                             clin-id (7 bytes)
    01                                       menu= Some
    00                                              List: 0 items

    0100 0000 0000 0000    length= 1 (64-bit LE)
    00                       body= Heartbeat
    ---   server -> client:   ---
    4d01 0000 0000 0000    length= 333 (64-bit LE)
    04                       body= Metadata
    01                             identification= Some
    0773 6572 762d 6964                             serv-id (7 bytes)
    01                                       menu= Some
    06                                              List: 6 items
    0874 6573 742d 7270    ...
    63                                              0: 1.    name= test-rpc (8 bytes)
    02                                                    version= 2 (int)
    00                                                 2. Rpc
    e2d2 61c6 c291 b94b    ...
    f6aa 68ec 2b08 cb00                                      query= (md5)
    e2d2 61c6 c291 b94b    ...
    f6aa 68ec 2b08 cb00                                   response= (md5)
    0d74 6573 742d 7069    ...
    7065 2d72 7063                                  1: 1.     name= test-pi... (13 bytes)
    01                                                     version= 1 (int)
    02                                                 2. Streaming_rpc
    e2d2 61c6 c291 b94b    ...
    f6aa 68ec 2b08 cb00                                              query= (md5)
    86ba 5df7 47ee c837    ...
    f0b3 91dd 49f3 3f9e                                   initial_response= (md5)
    e2d2 61c6 c291 b94b    ...
    f6aa 68ec 2b08 cb00                                    update_response= (md5)
    5296 6f4a 49a7 7bfd    ...
    ff66 8e9c c615 11b3                                              error= (md5)
    0e74 6573 742d 7374    ...
    6174 652d 7270 63                               2: 1.             name= test-st... (14 bytes)
    01                                                             version= 1 (int)
    02                                                 2. Streaming_rpc
    e2d2 61c6 c291 b94b    ...
    f6aa 68ec 2b08 cb00                                              query= (md5)
    e2d2 61c6 c291 b94b    ...
    f6aa 68ec 2b08 cb00                                   initial_response= (md5)
    e2d2 61c6 c291 b94b    ...
    f6aa 68ec 2b08 cb00                                    update_response= (md5)
    5296 6f4a 49a7 7bfd    ...
    ff66 8e9c c615 11b3                                              error= (md5)
    0874 6573 742d 7270    ...
    63                                              3: 1.             name= test-rpc (8 bytes)
    01                                                             version= 1 (int)
    00                                                 2. Rpc
    e2d2 61c6 c291 b94b    ...
    f6aa 68ec 2b08 cb00                                      query= (md5)
    e2d2 61c6 c291 b94b    ...
    f6aa 68ec 2b08 cb00                                   response= (md5)
    0473 6f72 74                                    4: 1.     name= sort (4 bytes)
    01                                                     version= 1 (int)
    00                                                 2. Rpc
    4c13 8035 aa69 ec9d    ...
    d8b7 a711 9090 f84a                                      query= (md5)
    4c13 8035 aa69 ec9d    ...
    d8b7 a711 9090 f84a                                   response= (md5)
    1074 6573 742d 6f6e    ...
    652d 7761 792d 7270    ...
    63                                              5: 1.     name= test-on... (16 bytes)
    01                                                     version= 1 (int)
    01                                                 2. One_way
    e2d2 61c6 c291 b94b    ...
    f6aa 68ec 2b08 cb00                                   msg= (md5)

    0100 0000 0000 0000    length= 1 (64-bit LE)
    00                       body= Heartbeat
    ((server_id_from_client (serv-id)) (client_id_from_server (clin-id)))
    |}];
  let%bind () =
    connection_test
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
      print_messages tap;
      [%expect
        {|
        1500 0000 0000 0000    length= 21 (64-bit LE)
        03                       body= Query
        0473 6f72 74                        tag= sort (4 bytes)
        01                              version= 1 (int)
        01                                   id= 1 (int)
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
        02                       body= Response
        01                               id= 1 (int)
        00                             data= Ok
        0b                                   length= 11 (int)
        04                                     body= Array: 4 items
        23                                           0: 35 (int)
        fea6 00                                      1: 166 (int)
        fe80 01                                      2: 384 (int)
        fee5 03                                      3: 997 (int)
        |}];
      let%bind _response = Rpc.Rpc.dispatch_exn rpc conn payload in
      print_messages tap;
      [%expect
        {|
        1500 0000 0000 0000    length= 21 (64-bit LE)
        03                       body= Query
        0473 6f72 74                        tag= sort (4 bytes)
        01                              version= 1 (int)
        02                                   id= 2 (int)
        00                             metadata= None
        0b                                 data= length= 11 (int)
        04                                         body= Array: 4 items
        fe80 01                                          0: 384 (int)
        fee5 03                                          1: 997 (int)
        fea6 00                                          2: 166 (int)
        23                                               3: 35 (int)

        0f00 0000 0000 0000    length= 15 (64-bit LE)
        02                       body= Response
        02                               id= 2 (int)
        00                             data= Ok
        0b                                   length= 11 (int)
        04                                     body= Array: 4 items
        23                                           0: 35 (int)
        fea6 00                                      1: 166 (int)
        fe80 01                                      2: 384 (int)
        fee5 03                                      3: 997 (int)
        |}];
      let%bind _response = Rpc.Rpc.dispatch_exn rpc conn payload in
      print_messages tap;
      [%expect
        {|
        1500 0000 0000 0000    length= 21 (64-bit LE)
        03                       body= Query
        0473 6f72 74                        tag= sort (4 bytes)
        01                              version= 1 (int)
        03                                   id= 3 (int)
        00                             metadata= None
        0b                                 data= length= 11 (int)
        04                                         body= Array: 4 items
        fe80 01                                          0: 384 (int)
        fee5 03                                          1: 997 (int)
        fea6 00                                          2: 166 (int)
        23                                               3: 35 (int)

        0f00 0000 0000 0000    length= 15 (64-bit LE)
        02                       body= Response
        03                               id= 3 (int)
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
      print_messages tap;
      [%expect
        {|
        1400 0000 0000 0000    length= 20 (64-bit LE)
        01                       body= Query_v1
        0473 6f72 74                       tag= sort (4 bytes)
        01                             version= 1 (int)
        04                                  id= 4 (int)
        0b                                data= length= 11 (int)
        04                                        body= Array: 4 items
        fe80 01                                         0: 384 (int)
        fee5 03                                         1: 997 (int)
        fea6 00                                         2: 166 (int)
        23                                              3: 35 (int)

        0100 0000 0000 0000    length= 1 (64-bit LE)
        00                       body= Heartbeat

        0f00 0000 0000 0000    length= 15 (64-bit LE)
        02                       body= Response
        04                               id= 4 (int)
        00                             data= Ok
        0b                                   length= 11 (int)
        04                                     body= Array: 4 items
        23                                           0: 35 (int)
        fea6 00                                      1: 166 (int)
        fe80 01                                      2: 384 (int)
        fee5 03                                      3: 997 (int)
        |}];
      let%bind _response = Rpc.Rpc.dispatch_exn Test_helpers.sort_rpc conn payload in
      print_messages tap;
      [%expect
        {|
        1400 0000 0000 0000    length= 20 (64-bit LE)
        01                       body= Query_v1
        0473 6f72 74                       tag= sort (4 bytes)
        01                             version= 1 (int)
        05                                  id= 5 (int)
        0b                                data= length= 11 (int)
        04                                        body= Array: 4 items
        fe80 01                                         0: 384 (int)
        fee5 03                                         1: 997 (int)
        fea6 00                                         2: 166 (int)
        23                                              3: 35 (int)

        0f00 0000 0000 0000    length= 15 (64-bit LE)
        02                       body= Response
        05                               id= 5 (int)
        00                             data= Ok
        0b                                   length= 11 (int)
        04                                     body= Array: 4 items
        23                                           0: 35 (int)
        fea6 00                                      1: 166 (int)
        fe80 01                                      2: 384 (int)
        fee5 03                                      3: 997 (int)
        |}];
      let%bind _response = Rpc.Rpc.dispatch_exn Test_helpers.sort_rpc conn payload in
      print_messages tap;
      [%expect
        {|
        1400 0000 0000 0000    length= 20 (64-bit LE)
        01                       body= Query_v1
        0473 6f72 74                       tag= sort (4 bytes)
        01                             version= 1 (int)
        06                                  id= 6 (int)
        0b                                data= length= 11 (int)
        04                                        body= Array: 4 items
        fe80 01                                         0: 384 (int)
        fee5 03                                         1: 997 (int)
        fea6 00                                         2: 166 (int)
        23                                              3: 35 (int)

        0f00 0000 0000 0000    length= 15 (64-bit LE)
        02                       body= Response
        06                               id= 6 (int)
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
    print_messages_bidirectional ~s_to_c ~c_to_s;
    [%expect
      {|
        ---   client -> server:   ---
        1400 0000 0000 0000    length= 20 (64-bit LE)
        01                       body= Query_v1
        0473 6f72 74                       tag= sort (4 bytes)
        01                             version= 1 (int)
        07                                  id= 7 (int)
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
        02                       body= Response
        07                               id= 7 (int)
        00                             data= Ok
        0b                                   length= 11 (int)
        04                                     body= Array: 4 items
        23                                           0: 35 (int)
        fea6 00                                      1: 166 (int)
        fe80 01                                      2: 384 (int)
        fee5 03                                      3: 997 (int)
        |}];
    let%bind () = dispatch ~client in
    print_messages_bidirectional ~s_to_c ~c_to_s;
    [%expect
      {|
        ---   client -> server:   ---
        0a00 0000 0000 0000    length= 10 (64-bit LE)
        01                       body= Query_v1
        0473 6f72 74                       tag= sort (4 bytes)
        01                             version= 1 (int)
        08                                  id= 8 (int)
        01                                data= length= 1 (int)
        00                                        body= Array: 0 items
        ---   server -> client:   ---
        0500 0000 0000 0000    length= 5 (64-bit LE)
        02                       body= Response
        08                               id= 8 (int)
        00                             data= Ok
        01                                   length= 1 (int)
        00                                     body= Array: 0 items
        |}];
    let%bind () = dispatch ~client in
    print_messages_bidirectional ~s_to_c ~c_to_s;
    [%expect
      {|
        ---   client -> server:   ---
        0a00 0000 0000 0000    length= 10 (64-bit LE)
        01                       body= Query_v1
        0473 6f72 74                       tag= sort (4 bytes)
        01                             version= 1 (int)
        09                                  id= 9 (int)
        01                                data= length= 1 (int)
        00                                        body= Array: 0 items
        ---   server -> client:   ---
        0500 0000 0000 0000    length= 5 (64-bit LE)
        02                       body= Response
        09                               id= 9 (int)
        00                             data= Ok
        01                                   length= 1 (int)
        00                                     body= Array: 0 items
        |}];
    return ())
;;

let%expect_test "[V2 -> V2] RPC connection" =
  Test_helpers.with_rpc_server_connection
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
    print_messages_bidirectional ~s_to_c ~c_to_s;
    [%expect
      {|
        ---   client -> server:   ---
        1500 0000 0000 0000    length= 21 (64-bit LE)
        03                       body= Query
        0473 6f72 74                        tag= sort (4 bytes)
        01                              version= 1 (int)
        0a                                   id= 10 (int)
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
        02                       body= Response
        0a                               id= 10 (int)
        00                             data= Ok
        0b                                   length= 11 (int)
        04                                     body= Array: 4 items
        23                                           0: 35 (int)
        fea6 00                                      1: 166 (int)
        fe80 01                                      2: 384 (int)
        fee5 03                                      3: 997 (int)
        |}];
    let%bind () = dispatch ~client in
    print_messages_bidirectional ~s_to_c ~c_to_s;
    [%expect
      {|
        ---   client -> server:   ---
        0b00 0000 0000 0000    length= 11 (64-bit LE)
        03                       body= Query
        0473 6f72 74                        tag= sort (4 bytes)
        01                              version= 1 (int)
        0b                                   id= 11 (int)
        00                             metadata= None
        01                                 data= length= 1 (int)
        00                                         body= Array: 0 items
        ---   server -> client:   ---
        0500 0000 0000 0000    length= 5 (64-bit LE)
        02                       body= Response
        0b                               id= 11 (int)
        00                             data= Ok
        01                                   length= 1 (int)
        00                                     body= Array: 0 items
        |}];
    let%bind () = dispatch ~client in
    print_messages_bidirectional ~s_to_c ~c_to_s;
    [%expect
      {|
        ---   client -> server:   ---
        0b00 0000 0000 0000    length= 11 (64-bit LE)
        03                       body= Query
        0473 6f72 74                        tag= sort (4 bytes)
        01                              version= 1 (int)
        0c                                   id= 12 (int)
        00                             metadata= None
        01                                 data= length= 1 (int)
        00                                         body= Array: 0 items
        ---   server -> client:   ---
        0500 0000 0000 0000    length= 5 (64-bit LE)
        02                       body= Response
        0c                               id= 12 (int)
        00                             data= Ok
        01                                   length= 1 (int)
        00                                     body= Array: 0 items
        |}];
    return ())
;;

(* Compatibility tests *)

let%expect_test "[V1 -> V2] RPC connection" =
  Test_helpers.with_rpc_server_connection
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
    print_messages_bidirectional ~s_to_c ~c_to_s;
    [%expect
      {|
        ---   client -> server:   ---
        1400 0000 0000 0000    length= 20 (64-bit LE)
        01                       body= Query_v1
        0473 6f72 74                       tag= sort (4 bytes)
        01                             version= 1 (int)
        0d                                  id= 13 (int)
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
        02                       body= Response
        0d                               id= 13 (int)
        00                             data= Ok
        0b                                   length= 11 (int)
        04                                     body= Array: 4 items
        23                                           0: 35 (int)
        fea6 00                                      1: 166 (int)
        fe80 01                                      2: 384 (int)
        fee5 03                                      3: 997 (int)
        |}];
    let%bind () = dispatch ~client in
    print_messages_bidirectional ~s_to_c ~c_to_s;
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
        02                       body= Response
        0e                               id= 14 (int)
        00                             data= Ok
        01                                   length= 1 (int)
        00                                     body= Array: 0 items
        |}];
    let%bind _ = dispatch ~client in
    print_messages_bidirectional ~s_to_c ~c_to_s;
    [%expect
      {|
        ---   client -> server:   ---
        0a00 0000 0000 0000    length= 10 (64-bit LE)
        01                       body= Query_v1
        0473 6f72 74                       tag= sort (4 bytes)
        01                             version= 1 (int)
        0f                                  id= 15 (int)
        01                                data= length= 1 (int)
        00                                        body= Array: 0 items
        ---   server -> client:   ---
        0500 0000 0000 0000    length= 5 (64-bit LE)
        02                       body= Response
        0f                               id= 15 (int)
        00                             data= Ok
        01                                   length= 1 (int)
        00                                     body= Array: 0 items
        |}];
    return ())
;;

let%expect_test "[V2 -> V1] RPC connection" =
  Test_helpers.with_rpc_server_connection
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
    print_messages_bidirectional ~s_to_c ~c_to_s;
    [%expect
      {|
        ---   client -> server:   ---
        1400 0000 0000 0000    length= 20 (64-bit LE)
        01                       body= Query_v1
        0473 6f72 74                       tag= sort (4 bytes)
        01                             version= 1 (int)
        10                                  id= 16 (int)
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
        02                       body= Response
        10                               id= 16 (int)
        00                             data= Ok
        0b                                   length= 11 (int)
        04                                     body= Array: 4 items
        23                                           0: 35 (int)
        fea6 00                                      1: 166 (int)
        fe80 01                                      2: 384 (int)
        fee5 03                                      3: 997 (int)
        |}];
    let%bind () = dispatch ~client in
    print_messages_bidirectional ~s_to_c ~c_to_s;
    [%expect
      {|
        ---   client -> server:   ---
        0a00 0000 0000 0000    length= 10 (64-bit LE)
        01                       body= Query_v1
        0473 6f72 74                       tag= sort (4 bytes)
        01                             version= 1 (int)
        11                                  id= 17 (int)
        01                                data= length= 1 (int)
        00                                        body= Array: 0 items
        ---   server -> client:   ---
        0500 0000 0000 0000    length= 5 (64-bit LE)
        02                       body= Response
        11                               id= 17 (int)
        00                             data= Ok
        01                                   length= 1 (int)
        00                                     body= Array: 0 items
        |}];
    let%bind () = dispatch ~client in
    print_messages_bidirectional ~s_to_c ~c_to_s;
    [%expect
      {|
        ---   client -> server:   ---
        0a00 0000 0000 0000    length= 10 (64-bit LE)
        01                       body= Query_v1
        0473 6f72 74                       tag= sort (4 bytes)
        01                             version= 1 (int)
        12                                  id= 18 (int)
        01                                data= length= 1 (int)
        00                                        body= Array: 0 items
        ---   server -> client:   ---
        0500 0000 0000 0000    length= 5 (64-bit LE)
        02                       body= Response
        12                               id= 18 (int)
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
      print_messages tap;
      [%expect
        {|
        1500 0000 0000 0000    length= 21 (64-bit LE)
        03                       body= Query
        0473 6f72 74                        tag= sort (4 bytes)
        01                              version= 1 (int)
        13                                   id= 19 (int)
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
        02                       body= Response
        13                               id= 19 (int)
        00                             data= Ok
        0b                                   length= 11 (int)
        04                                     body= Array: 4 items
        23                                           0: 35 (int)
        fea6 00                                      1: 166 (int)
        fe80 01                                      2: 384 (int)
        fee5 03                                      3: 997 (int)
        |}];
      let%bind _response = dispatch_expert ~client:conn ~payload in
      print_messages tap;
      [%expect
        {|
        1500 0000 0000 0000    length= 21 (64-bit LE)
        03                       body= Query
        0473 6f72 74                        tag= sort (4 bytes)
        01                              version= 1 (int)
        14                                   id= 20 (int)
        00                             metadata= None
        0b                                 data= length= 11 (int)
        04                                         body= Array: 4 items
        fe80 01                                          0: 384 (int)
        fee5 03                                          1: 997 (int)
        fea6 00                                          2: 166 (int)
        23                                               3: 35 (int)

        0f00 0000 0000 0000    length= 15 (64-bit LE)
        02                       body= Response
        14                               id= 20 (int)
        00                             data= Ok
        0b                                   length= 11 (int)
        04                                     body= Array: 4 items
        23                                           0: 35 (int)
        fea6 00                                      1: 166 (int)
        fe80 01                                      2: 384 (int)
        fee5 03                                      3: 997 (int)
        |}];
      let%bind _response = dispatch_expert ~client:conn ~payload in
      print_messages tap;
      [%expect
        {|
        1500 0000 0000 0000    length= 21 (64-bit LE)
        03                       body= Query
        0473 6f72 74                        tag= sort (4 bytes)
        01                              version= 1 (int)
        15                                   id= 21 (int)
        00                             metadata= None
        0b                                 data= length= 11 (int)
        04                                         body= Array: 4 items
        fe80 01                                          0: 384 (int)
        fee5 03                                          1: 997 (int)
        fea6 00                                          2: 166 (int)
        23                                               3: 35 (int)

        0f00 0000 0000 0000    length= 15 (64-bit LE)
        02                       body= Response
        15                               id= 21 (int)
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
      print_messages tap;
      [%expect
        {|
        1400 0000 0000 0000    length= 20 (64-bit LE)
        01                       body= Query_v1
        0473 6f72 74                       tag= sort (4 bytes)
        01                             version= 1 (int)
        16                                  id= 22 (int)
        0b                                data= length= 11 (int)
        04                                        body= Array: 4 items
        fe80 01                                         0: 384 (int)
        fee5 03                                         1: 997 (int)
        fea6 00                                         2: 166 (int)
        23                                              3: 35 (int)

        0100 0000 0000 0000    length= 1 (64-bit LE)
        00                       body= Heartbeat

        0f00 0000 0000 0000    length= 15 (64-bit LE)
        02                       body= Response
        16                               id= 22 (int)
        00                             data= Ok
        0b                                   length= 11 (int)
        04                                     body= Array: 4 items
        23                                           0: 35 (int)
        fea6 00                                      1: 166 (int)
        fe80 01                                      2: 384 (int)
        fee5 03                                      3: 997 (int)
        |}];
      let%bind (_ : unit) = dispatch_expert ~client:conn ~payload in
      print_messages tap;
      [%expect
        {|
        1400 0000 0000 0000    length= 20 (64-bit LE)
        01                       body= Query_v1
        0473 6f72 74                       tag= sort (4 bytes)
        01                             version= 1 (int)
        17                                  id= 23 (int)
        0b                                data= length= 11 (int)
        04                                        body= Array: 4 items
        fe80 01                                         0: 384 (int)
        fee5 03                                         1: 997 (int)
        fea6 00                                         2: 166 (int)
        23                                              3: 35 (int)

        0f00 0000 0000 0000    length= 15 (64-bit LE)
        02                       body= Response
        17                               id= 23 (int)
        00                             data= Ok
        0b                                   length= 11 (int)
        04                                     body= Array: 4 items
        23                                           0: 35 (int)
        fea6 00                                      1: 166 (int)
        fe80 01                                      2: 384 (int)
        fee5 03                                      3: 997 (int)
        |}];
      let%bind (_ : unit) = dispatch_expert ~client:conn ~payload in
      print_messages tap;
      [%expect
        {|
        1400 0000 0000 0000    length= 20 (64-bit LE)
        01                       body= Query_v1
        0473 6f72 74                       tag= sort (4 bytes)
        01                             version= 1 (int)
        18                                  id= 24 (int)
        0b                                data= length= 11 (int)
        04                                        body= Array: 4 items
        fe80 01                                         0: 384 (int)
        fee5 03                                         1: 997 (int)
        fea6 00                                         2: 166 (int)
        23                                              3: 35 (int)

        0f00 0000 0000 0000    length= 15 (64-bit LE)
        02                       body= Response
        18                               id= 24 (int)
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
    Rpc.Implementations.create_exn ~implementations:[] ~on_unknown_rpc:`Raise
  in
  let%bind server =
    Rpc.Connection.serve
      ~implementations:empty_implementations
      ~initial_connection_state:(fun _ (_ : Rpc.Connection.t) -> ())
      ~where_to_listen:Tcp.Where_to_listen.of_port_chosen_by_os
      ()
  in
  let connection_weak_pointer = Weak_pointer.create () in
  (* We try to ensure that [connection] doesn't leave the scope of this
     function except for [connection_weak_pointer], so that we can ensure it gets GC'ed
     with a Gc.major. *)
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
