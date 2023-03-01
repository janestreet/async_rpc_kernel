open! Core
open! Async

(* Helpers for manipulating handshake types *)
open struct
  module Header = Async_rpc_kernel.Async_rpc_kernel_private.Connection.For_testing.Header

  let with_handshake_header header =
    Async_rpc_kernel.Async_rpc_kernel_private.Connection.For_testing
    .with_async_execution_context
      ~context:header
  ;;
end

let rpc_tag = "test_rpc"
let version = 1

let rpc =
  Rpc.Rpc.create
    ~name:rpc_tag
    ~version
    ~bin_query:Bigstring.Stable.V1.bin_t
    ~bin_response:Bigstring.Stable.V1.bin_t
;;

module Payload = struct
  let get_random_size () = Byte_units.of_bytes_int (10 * Random.int 20)

  let create () =
    let size = get_random_size () in
    size
    |> Byte_units.bytes_int_exn
    |> Bigstring.init ~f:(fun (_ : int) -> Random.char ())
  ;;
end

let bytes_read conn = conn |> Rpc.Connection.bytes_read |> Byte_units.of_bytes_int63
let bytes_written conn = conn |> Rpc.Connection.bytes_written |> Byte_units.of_bytes_int63

let compare_bytes ~here a b =
  Expect_test_helpers_core.require_compare_equal here (module Byte_units) a b
;;

let expected_bytes conn ~expect_read ~expect_written ~here =
  let expect_read = Byte_units.of_bytes_int expect_read in
  let expect_written = Byte_units.of_bytes_int expect_written in
  let bytes_read = bytes_read conn in
  let bytes_written = bytes_written conn in
  compare_bytes ~here expect_read bytes_read;
  compare_bytes ~here expect_written bytes_written;
  print_s [%message (bytes_read : Byte_units.t) (bytes_written : Byte_units.t)]
;;

let expected_bytes_rw conn ~expect ~here =
  let expect = Byte_units.of_bytes_int expect in
  let bytes_read = bytes_read conn in
  let bytes_written = bytes_written conn in
  compare_bytes ~here expect bytes_read;
  compare_bytes ~here expect bytes_written;
  print_s [%message (bytes_read : Byte_units.t) (bytes_written : Byte_units.t)]
;;

let implementations =
  let implementation = Rpc.Rpc.implement rpc (fun () payload -> return payload) in
  Rpc.Implementations.create_exn
    ~implementations:[ implementation ]
    ~on_unknown_rpc:`Raise
;;

let with_local_connection ~header ~f =
  let%bind `Reader reader_fd, `Writer writer_fd =
    Unix.pipe (Info.of_string "rpc_test 1")
  in
  let reader = Reader.create reader_fd in
  let writer = Writer.create writer_fd in
  let%bind conn =
    with_handshake_header header ~f:(fun () ->
      Rpc.Connection.create ~implementations ~connection_state:(const ()) reader writer
      >>| Result.ok_exn)
  in
  let%bind result = f conn in
  let%bind () = Rpc.Connection.close conn in
  return result
;;

let only_heartbeat_once_at_the_beginning =
  Rpc.Connection.Heartbeat_config.create
    ~timeout:(Time_ns.Span.of_sec 360.)
    ~send_every:(Time_ns.Span.of_sec 120.)
    ()
;;

let with_rpc_server_connection ~server_header ~client_header ~f =
  let receiver_ivar = Ivar.create () in
  let%bind server =
    with_handshake_header server_header ~f:(fun () ->
      Rpc.Connection.serve
        ~heartbeat_config:only_heartbeat_once_at_the_beginning
        ~implementations
        ~initial_connection_state:(fun _ conn ->
          Ivar.fill receiver_ivar conn;
          ())
        ~where_to_listen:Tcp.Where_to_listen.of_port_chosen_by_os
        ())
  in
  let port = Tcp.Server.listening_on server in
  let where_to_connect =
    Tcp.Where_to_connect.of_host_and_port { Host_and_port.host = "localhost"; port }
  in
  let%bind sender =
    with_handshake_header client_header ~f:(fun () ->
      Rpc.Connection.client
        ~heartbeat_config:only_heartbeat_once_at_the_beginning
        where_to_connect
      >>| Result.ok_exn)
  in
  let%bind receiver = Ivar.read receiver_ivar in
  let%bind result = f ~sender ~receiver in
  let%bind () = Rpc.Connection.close sender in
  let%bind () = Tcp.Server.close server in
  return result
;;

module Snapshot = struct
  type t =
    { bytes_read : Byte_units.t
    ; bytes_written : Byte_units.t
    }
  [@@deriving sexp_of]

  let of_connection conn =
    let bytes_read = conn |> Rpc.Connection.bytes_read |> Byte_units.of_bytes_int63 in
    let bytes_written =
      conn |> Rpc.Connection.bytes_written |> Byte_units.of_bytes_int63
    in
    { bytes_read; bytes_written }
  ;;

  let print_diff tag prev next =
    let bytes_written = Byte_units.(next.bytes_written - prev.bytes_written) in
    let bytes_read = Byte_units.(next.bytes_read - prev.bytes_read) in
    print_s [%message tag (bytes_written : Byte_units.t) (bytes_read : Byte_units.t)]
  ;;
end

let measure_size ?header payload writer =
  Bin_prot.Utils.bin_dump ?header writer payload |> Bigstring.length
;;

let dispatch_and_verify_equal_sender_and_receiver_bytes
      ~here
      ~sender
      ~receiver
      ~sender_snapshot
      ~receiver_snapshot
  =
  let payload = Payload.create () in
  let%bind _response = Rpc.Rpc.dispatch_exn rpc sender payload in
  let next_sender_snapshot = Snapshot.of_connection sender in
  let next_receiver_snapshot = Snapshot.of_connection receiver in
  compare_bytes
    ~here
    sender_snapshot.Snapshot.bytes_read
    receiver_snapshot.Snapshot.bytes_written;
  compare_bytes
    ~here
    sender_snapshot.Snapshot.bytes_written
    receiver_snapshot.Snapshot.bytes_read;
  Snapshot.print_diff "Sender" sender_snapshot next_sender_snapshot;
  Snapshot.print_diff "Receiver" receiver_snapshot next_receiver_snapshot;
  return (next_sender_snapshot, next_receiver_snapshot)
;;

let dispatch_expert ~sender ~payload =
  let writer = [%bin_writer: Bigstring.Stable.V1.t] in
  let buf = Bin_prot.Utils.bin_dump writer payload in
  let wait_for_response = Ivar.create () in
  let handle_response _buf ~pos:_ ~len:_ =
    Ivar.fill wait_for_response ();
    return ()
  in
  match
    Rpc.Rpc.Expert.dispatch
      sender
      ~rpc_tag
      ~version
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

let v2_handshake_overhead = measure_size Header.v2 [%bin_writer: Header.t]
let v1_handshake_overhead = measure_size Header.v1 [%bin_writer: Header.t]

let individual_rpc_overhead =
  (* The overhead includes the tag, the query id, the version, empty metadata, and the
     query construction, wrapped in the request/response records. *)
  16
;;

let individual_legacy_rpc_overhead =
  (* The overhead includes the tag, the query id, the version, and the query construction,
     wrapped in the request/response records. *)
  15
;;

let next_expected_bytes_v2 ~previous ~payload =
  let payload = measure_size payload [%bin_writer: Bigstring.Stable.V1.t] in
  let payload_length = Bin_prot.Size.bin_size_int payload in
  previous
  + (payload_length * 2)
  + (payload * 2)
  (* Twice for send and response *) + individual_rpc_overhead
;;

let next_expected_bytes_v1 ~previous ~payload =
  let payload = measure_size payload [%bin_writer: Bigstring.Stable.V1.t] in
  let payload_length = Bin_prot.Size.bin_size_int payload in
  previous
  + (payload_length * 2)
  + (payload * 2)
  (* Twice for send and response *) + individual_legacy_rpc_overhead
;;

let%expect_test "bytes_read and bytes_write match over local rpc" =
  with_local_connection ~header:Header.v2 ~f:(fun conn ->
    let expect = v2_handshake_overhead in
    expected_bytes_rw conn ~expect ~here:[%here];
    [%expect {| ((bytes_read 8B) (bytes_written 8B)) |}];
    let payload = Payload.create () in
    let expect =
      let first_heartbeat_overhead = 1 in
      expect + first_heartbeat_overhead
    in
    let expect = next_expected_bytes_v2 ~previous:expect ~payload in
    let%bind _response = Rpc.Rpc.dispatch_exn rpc conn payload in
    expected_bytes_rw conn ~expect ~here:[%here];
    [%expect {| ((bytes_read 317B) (bytes_written 317B)) |}];
    let expect = next_expected_bytes_v2 ~previous:expect ~payload in
    let%bind _response = Rpc.Rpc.dispatch_exn rpc conn payload in
    expected_bytes_rw conn ~expect ~here:[%here];
    [%expect {| ((bytes_read 625B) (bytes_written 625B)) |}];
    let expect = next_expected_bytes_v2 ~previous:expect ~payload in
    let%bind _response = Rpc.Rpc.dispatch_exn rpc conn payload in
    expected_bytes_rw conn ~expect ~here:[%here];
    [%expect {| ((bytes_read 933B) (bytes_written 933B)) |}];
    return ())
;;

let%expect_test "[legacy] bytes_read and bytes_write match over local rpc" =
  with_local_connection ~header:Header.v1 ~f:(fun conn ->
    let expect = v1_handshake_overhead in
    expected_bytes_rw conn ~expect ~here:[%here];
    [%expect {| ((bytes_read 7B) (bytes_written 7B)) |}];
    let payload = Payload.create () in
    let expect =
      let first_heartbeat_overhead = 1 in
      expect + first_heartbeat_overhead
    in
    let expect = next_expected_bytes_v1 ~previous:expect ~payload in
    let%bind _response = Rpc.Rpc.dispatch_exn rpc conn payload in
    expected_bytes_rw conn ~expect ~here:[%here];
    [%expect {| ((bytes_read 315B) (bytes_written 315B)) |}];
    let expect = next_expected_bytes_v1 ~previous:expect ~payload in
    let%bind _response = Rpc.Rpc.dispatch_exn rpc conn payload in
    expected_bytes_rw conn ~expect ~here:[%here];
    [%expect {| ((bytes_read 622B) (bytes_written 622B)) |}];
    let expect = next_expected_bytes_v1 ~previous:expect ~payload in
    let%bind _response = Rpc.Rpc.dispatch_exn rpc conn payload in
    expected_bytes_rw conn ~expect ~here:[%here];
    [%expect {| ((bytes_read 929B) (bytes_written 929B)) |}];
    return ())
;;

let%expect_test "[V1 -> V1] bytes read and written over local rpc match on both ends" =
  with_rpc_server_connection
    ~client_header:Header.v1
    ~server_header:Header.v1
    ~f:(fun ~sender ~receiver ->
      let expect = v1_handshake_overhead in
      expected_bytes_rw sender ~expect ~here:[%here];
      [%expect {| ((bytes_read 7B) (bytes_written 7B)) |}];
      expected_bytes_rw receiver ~expect ~here:[%here];
      [%expect {| ((bytes_read 7B) (bytes_written 7B)) |}];
      let sender_snapshot = Snapshot.of_connection sender in
      let receiver_snapshot = Snapshot.of_connection receiver in
      let%bind sender_snapshot, receiver_snapshot =
        dispatch_and_verify_equal_sender_and_receiver_bytes
          ~here:[%here]
          ~sender
          ~receiver
          ~sender_snapshot
          ~receiver_snapshot
      in
      [%expect
        {|
        (Sender (bytes_written 159B) (bytes_read 150B))
        (Receiver (bytes_written 150B) (bytes_read 159B)) |}];
      let%bind sender_snapshot, receiver_snapshot =
        dispatch_and_verify_equal_sender_and_receiver_bytes
          ~here:[%here]
          ~sender
          ~receiver
          ~sender_snapshot
          ~receiver_snapshot
      in
      [%expect
        {|
        (Sender (bytes_written 64B) (bytes_read 55B))
        (Receiver (bytes_written 55B) (bytes_read 64B)) |}];
      let%bind _ =
        dispatch_and_verify_equal_sender_and_receiver_bytes
          ~here:[%here]
          ~sender
          ~receiver
          ~sender_snapshot
          ~receiver_snapshot
      in
      [%expect
        {|
        (Sender (bytes_written 114B) (bytes_read 105B))
        (Receiver (bytes_written 105B) (bytes_read 114B)) |}];
      return ())
;;

let%expect_test "[V2 -> V2] bytes read and written over local rpc match on both ends" =
  with_rpc_server_connection
    ~client_header:Header.v2
    ~server_header:Header.v2
    ~f:(fun ~sender ~receiver ->
      let expect = v2_handshake_overhead in
      expected_bytes_rw sender ~expect ~here:[%here];
      [%expect {| ((bytes_read 8B) (bytes_written 8B)) |}];
      expected_bytes_rw receiver ~expect ~here:[%here];
      [%expect {| ((bytes_read 8B) (bytes_written 8B)) |}];
      let sender_snapshot = Snapshot.of_connection sender in
      let receiver_snapshot = Snapshot.of_connection receiver in
      let%bind sender_snapshot, receiver_snapshot =
        dispatch_and_verify_equal_sender_and_receiver_bytes
          ~here:[%here]
          ~sender
          ~receiver
          ~sender_snapshot
          ~receiver_snapshot
      in
      [%expect
        {|
        (Sender (bytes_written 160B) (bytes_read 150B))
        (Receiver (bytes_written 150B) (bytes_read 160B)) |}];
      let%bind sender_snapshot, receiver_snapshot =
        dispatch_and_verify_equal_sender_and_receiver_bytes
          ~here:[%here]
          ~sender
          ~receiver
          ~sender_snapshot
          ~receiver_snapshot
      in
      [%expect
        {|
        (Sender (bytes_written 65B) (bytes_read 55B))
        (Receiver (bytes_written 55B) (bytes_read 65B)) |}];
      let%bind _ =
        dispatch_and_verify_equal_sender_and_receiver_bytes
          ~here:[%here]
          ~sender
          ~receiver
          ~sender_snapshot
          ~receiver_snapshot
      in
      [%expect
        {|
        (Sender (bytes_written 115B) (bytes_read 105B))
        (Receiver (bytes_written 105B) (bytes_read 115B)) |}];
      return ())
;;

(* Compatibility tests *)

let%expect_test "[V1 -> V2] bytes read and written over local rpc match on both ends" =
  with_rpc_server_connection
    ~client_header:Header.v1
    ~server_header:Header.v2
    ~f:(fun ~sender ~receiver ->
      expected_bytes
        sender
        ~expect_written:v1_handshake_overhead
        ~expect_read:v2_handshake_overhead
        ~here:[%here];
      [%expect {| ((bytes_read 8B) (bytes_written 7B)) |}];
      expected_bytes
        receiver
        ~expect_read:v1_handshake_overhead
        ~expect_written:v2_handshake_overhead
        ~here:[%here];
      [%expect {| ((bytes_read 7B) (bytes_written 8B)) |}];
      let sender_snapshot = Snapshot.of_connection sender in
      let receiver_snapshot = Snapshot.of_connection receiver in
      let%bind sender_snapshot, receiver_snapshot =
        dispatch_and_verify_equal_sender_and_receiver_bytes
          ~here:[%here]
          ~sender
          ~receiver
          ~sender_snapshot
          ~receiver_snapshot
      in
      [%expect
        {|
        (Sender (bytes_written 159B) (bytes_read 150B))
        (Receiver (bytes_written 150B) (bytes_read 159B)) |}];
      let%bind sender_snapshot, receiver_snapshot =
        dispatch_and_verify_equal_sender_and_receiver_bytes
          ~here:[%here]
          ~sender
          ~receiver
          ~sender_snapshot
          ~receiver_snapshot
      in
      [%expect
        {|
        (Sender (bytes_written 64B) (bytes_read 55B))
        (Receiver (bytes_written 55B) (bytes_read 64B)) |}];
      let%bind _ =
        dispatch_and_verify_equal_sender_and_receiver_bytes
          ~here:[%here]
          ~sender
          ~receiver
          ~sender_snapshot
          ~receiver_snapshot
      in
      [%expect
        {|
        (Sender (bytes_written 114B) (bytes_read 105B))
        (Receiver (bytes_written 105B) (bytes_read 114B)) |}];
      return ())
;;

let%expect_test "[V2 -> V1] bytes read and written over local rpc match on both ends" =
  with_rpc_server_connection
    ~client_header:Header.v2
    ~server_header:Header.v1
    ~f:(fun ~sender ~receiver ->
      expected_bytes
        sender
        ~expect_written:v2_handshake_overhead
        ~expect_read:v1_handshake_overhead
        ~here:[%here];
      [%expect {| ((bytes_read 7B) (bytes_written 8B)) |}];
      expected_bytes
        receiver
        ~expect_read:v2_handshake_overhead
        ~expect_written:v1_handshake_overhead
        ~here:[%here];
      [%expect {| ((bytes_read 8B) (bytes_written 7B)) |}];
      let sender_snapshot = Snapshot.of_connection sender in
      let receiver_snapshot = Snapshot.of_connection receiver in
      let%bind sender_snapshot, receiver_snapshot =
        dispatch_and_verify_equal_sender_and_receiver_bytes
          ~here:[%here]
          ~sender
          ~receiver
          ~sender_snapshot
          ~receiver_snapshot
      in
      [%expect
        {|
        (Sender (bytes_written 159B) (bytes_read 150B))
        (Receiver (bytes_written 150B) (bytes_read 159B)) |}];
      let%bind sender_snapshot, receiver_snapshot =
        dispatch_and_verify_equal_sender_and_receiver_bytes
          ~here:[%here]
          ~sender
          ~receiver
          ~sender_snapshot
          ~receiver_snapshot
      in
      [%expect
        {|
        (Sender (bytes_written 64B) (bytes_read 55B))
        (Receiver (bytes_written 55B) (bytes_read 64B)) |}];
      let%bind _ =
        dispatch_and_verify_equal_sender_and_receiver_bytes
          ~here:[%here]
          ~sender
          ~receiver
          ~sender_snapshot
          ~receiver_snapshot
      in
      [%expect
        {|
        (Sender (bytes_written 114B) (bytes_read 105B))
        (Receiver (bytes_written 105B) (bytes_read 114B)) |}];
      return ())
;;

let%expect_test "expert v2 sends expected # of bytes" =
  with_local_connection ~header:Header.v2 ~f:(fun conn ->
    let expect = v2_handshake_overhead in
    expected_bytes_rw conn ~expect ~here:[%here];
    [%expect {| ((bytes_read 8B) (bytes_written 8B)) |}];
    let payload = Payload.create () in
    let expect =
      let first_heartbeat_overhead = 1 in
      expect + first_heartbeat_overhead
    in
    let expect = next_expected_bytes_v2 ~previous:expect ~payload in
    let%bind _response = dispatch_expert ~sender:conn ~payload in
    expected_bytes_rw conn ~expect ~here:[%here];
    [%expect {| ((bytes_read 317B) (bytes_written 317B)) |}];
    let expect = next_expected_bytes_v2 ~previous:expect ~payload in
    let%bind _response = dispatch_expert ~sender:conn ~payload in
    expected_bytes_rw conn ~expect ~here:[%here];
    [%expect {| ((bytes_read 625B) (bytes_written 625B)) |}];
    let expect = next_expected_bytes_v2 ~previous:expect ~payload in
    let%bind _response = dispatch_expert ~sender:conn ~payload in
    expected_bytes_rw conn ~expect ~here:[%here];
    [%expect {| ((bytes_read 933B) (bytes_written 933B)) |}];
    return ())
;;

let%expect_test "expert v1 sends expected # of bytes" =
  with_local_connection ~header:Header.v1 ~f:(fun conn ->
    let expect = v1_handshake_overhead in
    expected_bytes_rw conn ~expect ~here:[%here];
    [%expect {| ((bytes_read 7B) (bytes_written 7B)) |}];
    let payload = Payload.create () in
    let expect =
      let first_heartbeat_overhead = 1 in
      expect + first_heartbeat_overhead
    in
    let expect = next_expected_bytes_v1 ~previous:expect ~payload in
    let%bind (_ : unit) = dispatch_expert ~sender:conn ~payload in
    expected_bytes_rw conn ~expect ~here:[%here];
    [%expect {| ((bytes_read 315B) (bytes_written 315B)) |}];
    let expect = next_expected_bytes_v1 ~previous:expect ~payload in
    let%bind (_ : unit) = dispatch_expert ~sender:conn ~payload in
    expected_bytes_rw conn ~expect ~here:[%here];
    [%expect {| ((bytes_read 622B) (bytes_written 622B)) |}];
    let expect = next_expected_bytes_v1 ~previous:expect ~payload in
    let%bind (_ : unit) = dispatch_expert ~sender:conn ~payload in
    expected_bytes_rw conn ~expect ~here:[%here];
    [%expect {| ((bytes_read 929B) (bytes_written 929B)) |}];
    return ())
;;
