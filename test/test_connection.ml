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
      02                             2: 2 (int) |}];
      let payload = Payload.create () in
      let rpc = Test_helpers.sort_rpc in
      let%bind _response = Rpc.Rpc.dispatch_exn rpc conn payload in
      print_messages tap;
      [%expect
        {|
        1500 0000 0000 0000    length= 21 (64-bit LE)
        03                       body= Query
        0473 6f72 74                         tag= sort (4 bytes)
        01                               version= 1 (int)
        01                                    id= 1 (int)
        00                              metadata= None
        0b                                  data= length= 11 (int)
        04                                          body= Array: 4 items
        fe14 02                                           0: 532 (int)
        44                                                1: 68 (int)
        feea 01                                           2: 490 (int)
        fed7 03                                           3: 983 (int)

        0100 0000 0000 0000    length= 1 (64-bit LE)
        00                       body= Heartbeat

        0f00 0000 0000 0000    length= 15 (64-bit LE)
        02                       body= Response
        01                                id= 1 (int)
        00                              data= Ok
        0b                                     length= 11 (int)
        04                                       body= Array: 4 items
        44                                             0: 68 (int)
        feea 01                                        1: 490 (int)
        fe14 02                                        2: 532 (int)
        fed7 03                                        3: 983 (int) |}];
      let%bind _response = Rpc.Rpc.dispatch_exn rpc conn payload in
      print_messages tap;
      [%expect
        {|
        1500 0000 0000 0000    length= 21 (64-bit LE)
        03                       body= Query
        0473 6f72 74                         tag= sort (4 bytes)
        01                               version= 1 (int)
        02                                    id= 2 (int)
        00                              metadata= None
        0b                                  data= length= 11 (int)
        04                                          body= Array: 4 items
        fe14 02                                           0: 532 (int)
        44                                                1: 68 (int)
        feea 01                                           2: 490 (int)
        fed7 03                                           3: 983 (int)

        0f00 0000 0000 0000    length= 15 (64-bit LE)
        02                       body= Response
        02                                id= 2 (int)
        00                              data= Ok
        0b                                     length= 11 (int)
        04                                       body= Array: 4 items
        44                                             0: 68 (int)
        feea 01                                        1: 490 (int)
        fe14 02                                        2: 532 (int)
        fed7 03                                        3: 983 (int) |}];
      let%bind _response = Rpc.Rpc.dispatch_exn rpc conn payload in
      print_messages tap;
      [%expect
        {|
        1500 0000 0000 0000    length= 21 (64-bit LE)
        03                       body= Query
        0473 6f72 74                         tag= sort (4 bytes)
        01                               version= 1 (int)
        03                                    id= 3 (int)
        00                              metadata= None
        0b                                  data= length= 11 (int)
        04                                          body= Array: 4 items
        fe14 02                                           0: 532 (int)
        44                                                1: 68 (int)
        feea 01                                           2: 490 (int)
        fed7 03                                           3: 983 (int)

        0f00 0000 0000 0000    length= 15 (64-bit LE)
        02                       body= Response
        03                                id= 3 (int)
        00                              data= Ok
        0b                                     length= 11 (int)
        04                                       body= Array: 4 items
        44                                             0: 68 (int)
        feea 01                                        1: 490 (int)
        fe14 02                                        2: 532 (int)
        fed7 03                                        3: 983 (int) |}];
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
      01                             1: 1 (int) |}];
      let payload = Payload.create () in
      let%bind _response = Rpc.Rpc.dispatch_exn Test_helpers.sort_rpc conn payload in
      print_messages tap;
      [%expect
        {|
        1400 0000 0000 0000    length= 20 (64-bit LE)
        01                       body= Query_v1
        0473 6f72 74                        tag= sort (4 bytes)
        01                              version= 1 (int)
        04                                   id= 4 (int)
        0b                                 data= length= 11 (int)
        04                                         body= Array: 4 items
        fe14 02                                          0: 532 (int)
        44                                               1: 68 (int)
        feea 01                                          2: 490 (int)
        fed7 03                                          3: 983 (int)

        0100 0000 0000 0000    length= 1 (64-bit LE)
        00                       body= Heartbeat

        0f00 0000 0000 0000    length= 15 (64-bit LE)
        02                       body= Response
        04                                id= 4 (int)
        00                              data= Ok
        0b                                     length= 11 (int)
        04                                       body= Array: 4 items
        44                                             0: 68 (int)
        feea 01                                        1: 490 (int)
        fe14 02                                        2: 532 (int)
        fed7 03                                        3: 983 (int) |}];
      let%bind _response = Rpc.Rpc.dispatch_exn Test_helpers.sort_rpc conn payload in
      print_messages tap;
      [%expect
        {|
        1400 0000 0000 0000    length= 20 (64-bit LE)
        01                       body= Query_v1
        0473 6f72 74                        tag= sort (4 bytes)
        01                              version= 1 (int)
        05                                   id= 5 (int)
        0b                                 data= length= 11 (int)
        04                                         body= Array: 4 items
        fe14 02                                          0: 532 (int)
        44                                               1: 68 (int)
        feea 01                                          2: 490 (int)
        fed7 03                                          3: 983 (int)

        0f00 0000 0000 0000    length= 15 (64-bit LE)
        02                       body= Response
        05                                id= 5 (int)
        00                              data= Ok
        0b                                     length= 11 (int)
        04                                       body= Array: 4 items
        44                                             0: 68 (int)
        feea 01                                        1: 490 (int)
        fe14 02                                        2: 532 (int)
        fed7 03                                        3: 983 (int) |}];
      let%bind _response = Rpc.Rpc.dispatch_exn Test_helpers.sort_rpc conn payload in
      print_messages tap;
      [%expect
        {|
        1400 0000 0000 0000    length= 20 (64-bit LE)
        01                       body= Query_v1
        0473 6f72 74                        tag= sort (4 bytes)
        01                              version= 1 (int)
        06                                   id= 6 (int)
        0b                                 data= length= 11 (int)
        04                                         body= Array: 4 items
        fe14 02                                          0: 532 (int)
        44                                               1: 68 (int)
        feea 01                                          2: 490 (int)
        fed7 03                                          3: 983 (int)

        0f00 0000 0000 0000    length= 15 (64-bit LE)
        02                       body= Response
        06                                id= 6 (int)
        00                              data= Ok
        0b                                     length= 11 (int)
        04                                       body= Array: 4 items
        44                                             0: 68 (int)
        feea 01                                        1: 490 (int)
        fe14 02                                        2: 532 (int)
        fed7 03                                        3: 983 (int) |}];
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
        01                             1: 1 (int) |}];
    let%bind () = dispatch ~client in
    print_messages_bidirectional ~s_to_c ~c_to_s;
    [%expect
      {|
        ---   client -> server:   ---
        1400 0000 0000 0000    length= 20 (64-bit LE)
        01                       body= Query_v1
        0473 6f72 74                        tag= sort (4 bytes)
        01                              version= 1 (int)
        07                                   id= 7 (int)
        0b                                 data= length= 11 (int)
        04                                         body= Array: 4 items
        fe14 02                                          0: 532 (int)
        44                                               1: 68 (int)
        feea 01                                          2: 490 (int)
        fed7 03                                          3: 983 (int)

        0100 0000 0000 0000    length= 1 (64-bit LE)
        00                       body= Heartbeat
        ---   server -> client:   ---
        0100 0000 0000 0000    length= 1 (64-bit LE)
        00                       body= Heartbeat

        0f00 0000 0000 0000    length= 15 (64-bit LE)
        02                       body= Response
        07                                id= 7 (int)
        00                              data= Ok
        0b                                     length= 11 (int)
        04                                       body= Array: 4 items
        44                                             0: 68 (int)
        feea 01                                        1: 490 (int)
        fe14 02                                        2: 532 (int)
        fed7 03                                        3: 983 (int) |}];
    let%bind () = dispatch ~client in
    print_messages_bidirectional ~s_to_c ~c_to_s;
    [%expect
      {|
        ---   client -> server:   ---
        1300 0000 0000 0000    length= 19 (64-bit LE)
        01                       body= Query_v1
        0473 6f72 74                        tag= sort (4 bytes)
        01                              version= 1 (int)
        08                                   id= 8 (int)
        0a                                 data= length= 10 (int)
        03                                         body= Array: 3 items
        fe6e 02                                          0: 622 (int)
        fecb 00                                          1: 203 (int)
        feb5 03                                          2: 949 (int)
        ---   server -> client:   ---
        0e00 0000 0000 0000    length= 14 (64-bit LE)
        02                       body= Response
        08                                id= 8 (int)
        00                              data= Ok
        0a                                     length= 10 (int)
        03                                       body= Array: 3 items
        fecb 00                                        0: 203 (int)
        fe6e 02                                        1: 622 (int)
        feb5 03                                        2: 949 (int) |}];
    let%bind () = dispatch ~client in
    print_messages_bidirectional ~s_to_c ~c_to_s;
    [%expect
      {|
        ---   client -> server:   ---
        0f00 0000 0000 0000    length= 15 (64-bit LE)
        01                       body= Query_v1
        0473 6f72 74                        tag= sort (4 bytes)
        01                              version= 1 (int)
        09                                   id= 9 (int)
        06                                 data= length= 6 (int)
        03                                         body= Array: 3 items
        74                                               0: 116 (int)
        37                                               1: 55 (int)
        fe18 02                                          2: 536 (int)
        ---   server -> client:   ---
        0a00 0000 0000 0000    length= 10 (64-bit LE)
        02                       body= Response
        09                                id= 9 (int)
        00                              data= Ok
        06                                     length= 6 (int)
        03                                       body= Array: 3 items
        37                                             0: 55 (int)
        74                                             1: 116 (int)
        fe18 02                                        2: 536 (int) |}];
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
        02                             2: 2 (int) |}];
    let%bind () = dispatch ~client in
    print_messages_bidirectional ~s_to_c ~c_to_s;
    [%expect
      {|
        ---   client -> server:   ---
        1500 0000 0000 0000    length= 21 (64-bit LE)
        03                       body= Query
        0473 6f72 74                         tag= sort (4 bytes)
        01                               version= 1 (int)
        0a                                    id= 10 (int)
        00                              metadata= None
        0b                                  data= length= 11 (int)
        04                                          body= Array: 4 items
        fe14 02                                           0: 532 (int)
        44                                                1: 68 (int)
        feea 01                                           2: 490 (int)
        fed7 03                                           3: 983 (int)

        0100 0000 0000 0000    length= 1 (64-bit LE)
        00                       body= Heartbeat
        ---   server -> client:   ---
        0100 0000 0000 0000    length= 1 (64-bit LE)
        00                       body= Heartbeat

        0f00 0000 0000 0000    length= 15 (64-bit LE)
        02                       body= Response
        0a                                id= 10 (int)
        00                              data= Ok
        0b                                     length= 11 (int)
        04                                       body= Array: 4 items
        44                                             0: 68 (int)
        feea 01                                        1: 490 (int)
        fe14 02                                        2: 532 (int)
        fed7 03                                        3: 983 (int) |}];
    let%bind () = dispatch ~client in
    print_messages_bidirectional ~s_to_c ~c_to_s;
    [%expect
      {|
        ---   client -> server:   ---
        1400 0000 0000 0000    length= 20 (64-bit LE)
        03                       body= Query
        0473 6f72 74                         tag= sort (4 bytes)
        01                               version= 1 (int)
        0b                                    id= 11 (int)
        00                              metadata= None
        0a                                  data= length= 10 (int)
        03                                          body= Array: 3 items
        fe6e 02                                           0: 622 (int)
        fecb 00                                           1: 203 (int)
        feb5 03                                           2: 949 (int)
        ---   server -> client:   ---
        0e00 0000 0000 0000    length= 14 (64-bit LE)
        02                       body= Response
        0b                                id= 11 (int)
        00                              data= Ok
        0a                                     length= 10 (int)
        03                                       body= Array: 3 items
        fecb 00                                        0: 203 (int)
        fe6e 02                                        1: 622 (int)
        feb5 03                                        2: 949 (int) |}];
    let%bind () = dispatch ~client in
    print_messages_bidirectional ~s_to_c ~c_to_s;
    [%expect
      {|
        ---   client -> server:   ---
        1000 0000 0000 0000    length= 16 (64-bit LE)
        03                       body= Query
        0473 6f72 74                         tag= sort (4 bytes)
        01                               version= 1 (int)
        0c                                    id= 12 (int)
        00                              metadata= None
        06                                  data= length= 6 (int)
        03                                          body= Array: 3 items
        74                                                0: 116 (int)
        37                                                1: 55 (int)
        fe18 02                                           2: 536 (int)
        ---   server -> client:   ---
        0a00 0000 0000 0000    length= 10 (64-bit LE)
        02                       body= Response
        0c                                id= 12 (int)
        00                              data= Ok
        06                                     length= 6 (int)
        03                                       body= Array: 3 items
        37                                             0: 55 (int)
        74                                             1: 116 (int)
        fe18 02                                        2: 536 (int) |}];
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
        02                             2: 2 (int) |}];
    let%bind () = dispatch ~client in
    print_messages_bidirectional ~s_to_c ~c_to_s;
    [%expect
      {|
        ---   client -> server:   ---
        1400 0000 0000 0000    length= 20 (64-bit LE)
        01                       body= Query_v1
        0473 6f72 74                        tag= sort (4 bytes)
        01                              version= 1 (int)
        0d                                   id= 13 (int)
        0b                                 data= length= 11 (int)
        04                                         body= Array: 4 items
        fe14 02                                          0: 532 (int)
        44                                               1: 68 (int)
        feea 01                                          2: 490 (int)
        fed7 03                                          3: 983 (int)

        0100 0000 0000 0000    length= 1 (64-bit LE)
        00                       body= Heartbeat
        ---   server -> client:   ---
        0100 0000 0000 0000    length= 1 (64-bit LE)
        00                       body= Heartbeat

        0f00 0000 0000 0000    length= 15 (64-bit LE)
        02                       body= Response
        0d                                id= 13 (int)
        00                              data= Ok
        0b                                     length= 11 (int)
        04                                       body= Array: 4 items
        44                                             0: 68 (int)
        feea 01                                        1: 490 (int)
        fe14 02                                        2: 532 (int)
        fed7 03                                        3: 983 (int) |}];
    let%bind () = dispatch ~client in
    print_messages_bidirectional ~s_to_c ~c_to_s;
    [%expect
      {|
        ---   client -> server:   ---
        1300 0000 0000 0000    length= 19 (64-bit LE)
        01                       body= Query_v1
        0473 6f72 74                        tag= sort (4 bytes)
        01                              version= 1 (int)
        0e                                   id= 14 (int)
        0a                                 data= length= 10 (int)
        03                                         body= Array: 3 items
        fe6e 02                                          0: 622 (int)
        fecb 00                                          1: 203 (int)
        feb5 03                                          2: 949 (int)
        ---   server -> client:   ---
        0e00 0000 0000 0000    length= 14 (64-bit LE)
        02                       body= Response
        0e                                id= 14 (int)
        00                              data= Ok
        0a                                     length= 10 (int)
        03                                       body= Array: 3 items
        fecb 00                                        0: 203 (int)
        fe6e 02                                        1: 622 (int)
        feb5 03                                        2: 949 (int) |}];
    let%bind _ = dispatch ~client in
    print_messages_bidirectional ~s_to_c ~c_to_s;
    [%expect
      {|
        ---   client -> server:   ---
        0f00 0000 0000 0000    length= 15 (64-bit LE)
        01                       body= Query_v1
        0473 6f72 74                        tag= sort (4 bytes)
        01                              version= 1 (int)
        0f                                   id= 15 (int)
        06                                 data= length= 6 (int)
        03                                         body= Array: 3 items
        74                                               0: 116 (int)
        37                                               1: 55 (int)
        fe18 02                                          2: 536 (int)
        ---   server -> client:   ---
        0a00 0000 0000 0000    length= 10 (64-bit LE)
        02                       body= Response
        0f                                id= 15 (int)
        00                              data= Ok
        06                                     length= 6 (int)
        03                                       body= Array: 3 items
        37                                             0: 55 (int)
        74                                             1: 116 (int)
        fe18 02                                        2: 536 (int) |}];
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
        01                             1: 1 (int) |}];
    let%bind () = dispatch ~client in
    print_messages_bidirectional ~s_to_c ~c_to_s;
    [%expect
      {|
        ---   client -> server:   ---
        1400 0000 0000 0000    length= 20 (64-bit LE)
        01                       body= Query_v1
        0473 6f72 74                        tag= sort (4 bytes)
        01                              version= 1 (int)
        10                                   id= 16 (int)
        0b                                 data= length= 11 (int)
        04                                         body= Array: 4 items
        fe14 02                                          0: 532 (int)
        44                                               1: 68 (int)
        feea 01                                          2: 490 (int)
        fed7 03                                          3: 983 (int)

        0100 0000 0000 0000    length= 1 (64-bit LE)
        00                       body= Heartbeat
        ---   server -> client:   ---
        0100 0000 0000 0000    length= 1 (64-bit LE)
        00                       body= Heartbeat

        0f00 0000 0000 0000    length= 15 (64-bit LE)
        02                       body= Response
        10                                id= 16 (int)
        00                              data= Ok
        0b                                     length= 11 (int)
        04                                       body= Array: 4 items
        44                                             0: 68 (int)
        feea 01                                        1: 490 (int)
        fe14 02                                        2: 532 (int)
        fed7 03                                        3: 983 (int) |}];
    let%bind () = dispatch ~client in
    print_messages_bidirectional ~s_to_c ~c_to_s;
    [%expect
      {|
        ---   client -> server:   ---
        1300 0000 0000 0000    length= 19 (64-bit LE)
        01                       body= Query_v1
        0473 6f72 74                        tag= sort (4 bytes)
        01                              version= 1 (int)
        11                                   id= 17 (int)
        0a                                 data= length= 10 (int)
        03                                         body= Array: 3 items
        fe6e 02                                          0: 622 (int)
        fecb 00                                          1: 203 (int)
        feb5 03                                          2: 949 (int)
        ---   server -> client:   ---
        0e00 0000 0000 0000    length= 14 (64-bit LE)
        02                       body= Response
        11                                id= 17 (int)
        00                              data= Ok
        0a                                     length= 10 (int)
        03                                       body= Array: 3 items
        fecb 00                                        0: 203 (int)
        fe6e 02                                        1: 622 (int)
        feb5 03                                        2: 949 (int) |}];
    let%bind () = dispatch ~client in
    print_messages_bidirectional ~s_to_c ~c_to_s;
    [%expect
      {|
        ---   client -> server:   ---
        0f00 0000 0000 0000    length= 15 (64-bit LE)
        01                       body= Query_v1
        0473 6f72 74                        tag= sort (4 bytes)
        01                              version= 1 (int)
        12                                   id= 18 (int)
        06                                 data= length= 6 (int)
        03                                         body= Array: 3 items
        74                                               0: 116 (int)
        37                                               1: 55 (int)
        fe18 02                                          2: 536 (int)
        ---   server -> client:   ---
        0a00 0000 0000 0000    length= 10 (64-bit LE)
        02                       body= Response
        12                                id= 18 (int)
        00                              data= Ok
        06                                     length= 6 (int)
        03                                       body= Array: 3 items
        37                                             0: 55 (int)
        74                                             1: 116 (int)
        fe18 02                                        2: 536 (int) |}];
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
      02                             2: 2 (int) |}];
      let payload = Payload.create () in
      let%bind _response = dispatch_expert ~client:conn ~payload in
      print_messages tap;
      [%expect
        {|
        1500 0000 0000 0000    length= 21 (64-bit LE)
        03                       body= Query
        0473 6f72 74                         tag= sort (4 bytes)
        01                               version= 1 (int)
        13                                    id= 19 (int)
        00                              metadata= None
        0b                                  data= length= 11 (int)
        04                                          body= Array: 4 items
        fe14 02                                           0: 532 (int)
        44                                                1: 68 (int)
        feea 01                                           2: 490 (int)
        fed7 03                                           3: 983 (int)

        0100 0000 0000 0000    length= 1 (64-bit LE)
        00                       body= Heartbeat

        0f00 0000 0000 0000    length= 15 (64-bit LE)
        02                       body= Response
        13                                id= 19 (int)
        00                              data= Ok
        0b                                     length= 11 (int)
        04                                       body= Array: 4 items
        44                                             0: 68 (int)
        feea 01                                        1: 490 (int)
        fe14 02                                        2: 532 (int)
        fed7 03                                        3: 983 (int) |}];
      let%bind _response = dispatch_expert ~client:conn ~payload in
      print_messages tap;
      [%expect
        {|
        1500 0000 0000 0000    length= 21 (64-bit LE)
        03                       body= Query
        0473 6f72 74                         tag= sort (4 bytes)
        01                               version= 1 (int)
        14                                    id= 20 (int)
        00                              metadata= None
        0b                                  data= length= 11 (int)
        04                                          body= Array: 4 items
        fe14 02                                           0: 532 (int)
        44                                                1: 68 (int)
        feea 01                                           2: 490 (int)
        fed7 03                                           3: 983 (int)

        0f00 0000 0000 0000    length= 15 (64-bit LE)
        02                       body= Response
        14                                id= 20 (int)
        00                              data= Ok
        0b                                     length= 11 (int)
        04                                       body= Array: 4 items
        44                                             0: 68 (int)
        feea 01                                        1: 490 (int)
        fe14 02                                        2: 532 (int)
        fed7 03                                        3: 983 (int) |}];
      let%bind _response = dispatch_expert ~client:conn ~payload in
      print_messages tap;
      [%expect
        {|
        1500 0000 0000 0000    length= 21 (64-bit LE)
        03                       body= Query
        0473 6f72 74                         tag= sort (4 bytes)
        01                               version= 1 (int)
        15                                    id= 21 (int)
        00                              metadata= None
        0b                                  data= length= 11 (int)
        04                                          body= Array: 4 items
        fe14 02                                           0: 532 (int)
        44                                                1: 68 (int)
        feea 01                                           2: 490 (int)
        fed7 03                                           3: 983 (int)

        0f00 0000 0000 0000    length= 15 (64-bit LE)
        02                       body= Response
        15                                id= 21 (int)
        00                              data= Ok
        0b                                     length= 11 (int)
        04                                       body= Array: 4 items
        44                                             0: 68 (int)
        feea 01                                        1: 490 (int)
        fe14 02                                        2: 532 (int)
        fed7 03                                        3: 983 (int) |}];
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
      01                             1: 1 (int) |}];
      let payload = Payload.create () in
      let%bind (_ : unit) = dispatch_expert ~client:conn ~payload in
      print_messages tap;
      [%expect
        {|
        1400 0000 0000 0000    length= 20 (64-bit LE)
        01                       body= Query_v1
        0473 6f72 74                        tag= sort (4 bytes)
        01                              version= 1 (int)
        16                                   id= 22 (int)
        0b                                 data= length= 11 (int)
        04                                         body= Array: 4 items
        fe14 02                                          0: 532 (int)
        44                                               1: 68 (int)
        feea 01                                          2: 490 (int)
        fed7 03                                          3: 983 (int)

        0100 0000 0000 0000    length= 1 (64-bit LE)
        00                       body= Heartbeat

        0f00 0000 0000 0000    length= 15 (64-bit LE)
        02                       body= Response
        16                                id= 22 (int)
        00                              data= Ok
        0b                                     length= 11 (int)
        04                                       body= Array: 4 items
        44                                             0: 68 (int)
        feea 01                                        1: 490 (int)
        fe14 02                                        2: 532 (int)
        fed7 03                                        3: 983 (int) |}];
      let%bind (_ : unit) = dispatch_expert ~client:conn ~payload in
      print_messages tap;
      [%expect
        {|
        1400 0000 0000 0000    length= 20 (64-bit LE)
        01                       body= Query_v1
        0473 6f72 74                        tag= sort (4 bytes)
        01                              version= 1 (int)
        17                                   id= 23 (int)
        0b                                 data= length= 11 (int)
        04                                         body= Array: 4 items
        fe14 02                                          0: 532 (int)
        44                                               1: 68 (int)
        feea 01                                          2: 490 (int)
        fed7 03                                          3: 983 (int)

        0f00 0000 0000 0000    length= 15 (64-bit LE)
        02                       body= Response
        17                                id= 23 (int)
        00                              data= Ok
        0b                                     length= 11 (int)
        04                                       body= Array: 4 items
        44                                             0: 68 (int)
        feea 01                                        1: 490 (int)
        fe14 02                                        2: 532 (int)
        fed7 03                                        3: 983 (int) |}];
      let%bind (_ : unit) = dispatch_expert ~client:conn ~payload in
      print_messages tap;
      [%expect
        {|
        1400 0000 0000 0000    length= 20 (64-bit LE)
        01                       body= Query_v1
        0473 6f72 74                        tag= sort (4 bytes)
        01                              version= 1 (int)
        18                                   id= 24 (int)
        0b                                 data= length= 11 (int)
        04                                         body= Array: 4 items
        fe14 02                                          0: 532 (int)
        44                                               1: 68 (int)
        feea 01                                          2: 490 (int)
        fed7 03                                          3: 983 (int)

        0f00 0000 0000 0000    length= 15 (64-bit LE)
        02                       body= Response
        18                                id= 24 (int)
        00                              data= Ok
        0b                                     length= 11 (int)
        04                                       body= Array: 4 items
        44                                             0: 68 (int)
        feea 01                                        1: 490 (int)
        fe14 02                                        2: 532 (int)
        fed7 03                                        3: 983 (int) |}];
      return ())
    ()
;;
