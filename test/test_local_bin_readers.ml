open! Core
open! Import

module Payload = struct
  type t = { field : string } [@@deriving bin_io ~localize]

  let bin_writer_t = Writer_with_length.of_writer [%bin_writer: t]
end

let parse_message_locally_and_show_allocations message =
  let writer = Protocol.Message.bin_writer_maybe_needs_length Payload.bin_writer_t in
  let size = writer.size message in
  let buf = Bigstring.create size in
  let written = writer.write buf ~pos:0 message in
  [%test_result: int] written ~expect:size;
  let pos_ref = ref 0 in
  let ( result
      , { Gc.For_testing.Allocation_report.major_words_allocated; minor_words_allocated }
      , (_ : Core.Gc.For_testing.Allocation_log.t list) )
    =
    Gc.For_testing.measure_and_log_allocation_local (fun () ->
      Protocol_local_readers.Message.bin_read_nat0_t__local buf ~pos_ref)
  in
  let result = [%globalize: Protocol.Message.nat0_t] result in
  print_endline "Parsed message:";
  print_s [%sexp (result : _ Protocol.Message.t)];
  if major_words_allocated > 0 || minor_words_allocated > 0
  then print_endline "bin_read allocated"
  else print_endline "bin_read did not allocate";
  print_endline ""
;;

let (all_messages : Payload.t Protocol.Message.t list) =
  let tag = Protocol.Rpc_tag.of_string "test-tag" in
  let id = Protocol.Query_id.of_int_exn 1 in
  let impl_menu_index = Protocol.Impl_menu_index.some (Bin_prot.Nat0.of_int 5) in
  let data = { Payload.field = "hello world" } in
  let menu =
    [ ( { Rpc.Description.name = "my-rpc"; version = 1 }
      , Async_rpc_kernel.Rpc_shapes.Just_digests.One_way
          { msg = Bin_shape.Digest.of_md5 (Md5.digest_string "my-digest") } )
    ]
  in
  let identification = Some (Bigstring.of_string "my client") in
  [ Heartbeat
  ; Query_v1 { tag; version = 10; id; data }
  ; Response_v1 { id; data = Ok data }
  ; Response_v2 { id; impl_menu_index = Protocol.Impl_menu_index.none; data = Ok data }
  ; Response_v2 { id; impl_menu_index; data = Ok data }
  ; Response_v2 { id; impl_menu_index; data = Ok data }
  ; Response_v2
      { id
      ; impl_menu_index
      ; data = Error (Write_error [%message "error happened" ~_:(42 : int)])
      }
  ; Response_v2
      { id
      ; impl_menu_index
      ; data = Error (Unimplemented_rpc (Protocol.Rpc_tag.of_string "a tag", `Version 77))
      }
  ; Query { tag; version = 10; id; metadata = Some "test metadata"; data }
    (* This allocates: bigstring cannot be allocated on the stack *)
  ; Metadata { identification; menu = Some menu }
    (* This allocates: Info.t is a global ref, and must be allocated globally *)
  ; Close_reason (Info.create_s [%message "my sexp info"])
    (* This allocates: bigstrings cannot be allocated on the stack, and mutable arrays
       cannot have their elements allocated on the stack *)
  ; Metadata_v2 { identification; menu = Some (Menu.of_supported_rpcs_and_shapes menu) }
  ]
;;

let%expect_test "Test reading different kinds of messages" =
  List.iter all_messages ~f:parse_message_locally_and_show_allocations;
  [%expect
    {|
    Parsed message:
    Heartbeat
    bin_read did not allocate

    Parsed message:
    (Query_v1 ((tag test-tag) (version 10) (id 1) (data _)))
    bin_read did not allocate

    Parsed message:
    (Response_v1 ((id 1) (data (Ok _))))
    bin_read did not allocate

    Parsed message:
    (Response_v2 ((id 1) (impl_menu_index ()) (data (Ok _))))
    bin_read did not allocate

    Parsed message:
    (Response_v2 ((id 1) (impl_menu_index (5)) (data (Ok _))))
    bin_read did not allocate

    Parsed message:
    (Response_v2 ((id 1) (impl_menu_index (5)) (data (Ok _))))
    bin_read did not allocate

    Parsed message:
    (Response_v2
     ((id 1) (impl_menu_index (5))
      (data (Error (Write_error ("error happened" 42))))))
    bin_read did not allocate

    Parsed message:
    (Response_v2
     ((id 1) (impl_menu_index (5))
      (data (Error (Unimplemented_rpc "a tag" (Version 77))))))
    bin_read did not allocate

    Parsed message:
    (Query
     ((tag test-tag) (version 10) (id 1) (metadata ("test metadata")) (data _)))
    bin_read did not allocate

    Parsed message:
    (Metadata
     ((identification ("my client"))
      (menu
       (((((name my-rpc) (version 1))
          (One_way (msg dcc5e16520e06e38ccb56d065b8f46f0))))))))
    bin_read allocated

    Parsed message:
    (Close_reason "my sexp info")
    bin_read allocated

    Parsed message:
    (Metadata_v2
     ((identification ("my client"))
      (menu
       (((descriptions (((name my-rpc) (version 1))))
         (digests (((One_way (msg dcc5e16520e06e38ccb56d065b8f46f0))))))))))
    bin_read allocated
    |}]
;;

module Header = Connection.For_testing.Header

let%expect_test "Test read protocol version header locally" =
  let header = Header.v6 in
  let buf = Bigstring.create ([%bin_size: Header.t] header) in
  let (_ : int) = [%bin_write: Header.t] buf ~pos:0 header in
  let pos_ref = ref 0 in
  Expect_test_helpers_core.require_no_allocation_local (fun () ->
    Header.bin_read_t__local buf ~pos_ref)
  |> [%globalize: Header.t]
  |> [%sexp_of: Header.t]
  |> print_s;
  [%expect {| (4411474 1 2 3 4 5 6) |}];
  ()
;;
