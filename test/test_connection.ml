open! Core
open! Async

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
  let%bind _response = Rpc.Rpc.dispatch_exn Test_helpers.rpc sender payload in
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
    Ivar.fill_exn wait_for_response ();
    return ()
  in
  match
    Rpc.Rpc.Expert.dispatch
      sender
      ~rpc_tag:Test_helpers.rpc_tag
      ~version:Test_helpers.rpc_version
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

let v2_handshake_overhead =
  measure_size Test_helpers.Header.v2 [%bin_writer: Test_helpers.Header.t]
;;

let v1_handshake_overhead =
  measure_size Test_helpers.Header.v1 [%bin_writer: Test_helpers.Header.t]
;;

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
  Test_helpers.with_local_connection
    ~header:Test_helpers.Header.v2
    ~f:(fun conn ->
      let expect = v2_handshake_overhead in
      expected_bytes_rw conn ~expect ~here:[%here];
      [%expect {|((bytes_read 8B) (bytes_written 8B))|}];
      let payload = Payload.create () in
      let expect =
        let first_heartbeat_overhead = 1 in
        expect + first_heartbeat_overhead
      in
      let rpc = Test_helpers.rpc in
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
    ()
;;

let%expect_test "[legacy] bytes_read and bytes_write match over local rpc" =
  Test_helpers.with_local_connection
    ~header:Test_helpers.Header.v1
    ~f:(fun conn ->
      let expect = v1_handshake_overhead in
      expected_bytes_rw conn ~expect ~here:[%here];
      [%expect {| ((bytes_read 7B) (bytes_written 7B)) |}];
      let payload = Payload.create () in
      let expect =
        let first_heartbeat_overhead = 1 in
        expect + first_heartbeat_overhead
      in
      let expect = next_expected_bytes_v1 ~previous:expect ~payload in
      let%bind _response = Rpc.Rpc.dispatch_exn Test_helpers.rpc conn payload in
      expected_bytes_rw conn ~expect ~here:[%here];
      [%expect {| ((bytes_read 315B) (bytes_written 315B)) |}];
      let expect = next_expected_bytes_v1 ~previous:expect ~payload in
      let%bind _response = Rpc.Rpc.dispatch_exn Test_helpers.rpc conn payload in
      expected_bytes_rw conn ~expect ~here:[%here];
      [%expect {| ((bytes_read 622B) (bytes_written 622B)) |}];
      let expect = next_expected_bytes_v1 ~previous:expect ~payload in
      let%bind _response = Rpc.Rpc.dispatch_exn Test_helpers.rpc conn payload in
      expected_bytes_rw conn ~expect ~here:[%here];
      [%expect {| ((bytes_read 929B) (bytes_written 929B)) |}];
      return ())
    ()
;;

let%expect_test "[V1 -> V1] bytes read and written over local rpc match on both ends" =
  Test_helpers.with_rpc_server_connection
    ~client_header:Test_helpers.Header.v1
    ~server_header:Test_helpers.Header.v1
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
  Test_helpers.with_rpc_server_connection
    ~client_header:Test_helpers.Header.v2
    ~server_header:Test_helpers.Header.v2
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
  Test_helpers.with_rpc_server_connection
    ~client_header:Test_helpers.Header.v1
    ~server_header:Test_helpers.Header.v2
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
  Test_helpers.with_rpc_server_connection
    ~client_header:Test_helpers.Header.v2
    ~server_header:Test_helpers.Header.v1
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
  Test_helpers.with_local_connection
    ~header:Test_helpers.Header.v2
    ~f:(fun conn ->
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
    ()
;;

let%expect_test "expert v1 sends expected # of bytes" =
  Test_helpers.with_local_connection
    ~header:Test_helpers.Header.v1
    ~f:(fun conn ->
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
    ()
;;
