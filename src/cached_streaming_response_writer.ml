open! Core
open! Async_kernel

type 'a t =
  { header_prefix : string (* Bin_protted constant prefix of the message *)
  ; (* Length of the user data part. We set this field when sending a message. This relies
       on the fact that the message is serialized immediately (which is the only
       acceptable semantics for the transport layer anyway, as it doesn't know if the
       value is mutable or not).

       [data_len] is passed to bin-prot writers by mutating [data_len] instead of by
       passing an additional argument to avoid some allocation.
    *)
    mutable data_len : Nat0.t
  ; bin_writer : 'a Bin_prot.Type_class.writer
  ; protocol_writer : Protocol_writer.t
  ; id : Protocol.Query_id.t
  ; impl_menu_index : Nat0.Option.t
  ; description : Description.t
  }

type void = Void

let bin_size_void Void = 0
let bin_write_void _buf ~pos Void = pos

type void_message = void Protocol.Message.maybe_needs_length [@@deriving bin_write]

type void_stream_response_data = void Protocol.Stream_response_data.needs_length
[@@deriving bin_write]

(* This is not re-entrant but Async code always runs on one thread at a time *)
let buffer = Bigstring.create 32

let cache_bin_protted (bin_writer : _ Bin_prot.Type_class.writer) x =
  let len = bin_writer.write buffer ~pos:0 x in
  Bigstring.To_string.sub buffer ~pos:0 ~len
;;

let create (type a) protocol_writer id impl_menu_index description ~bin_writer : a t =
  let header_prefix =
    cache_bin_protted
      bin_writer_void_message
      (Protocol_writer.Unsafe_for_cached_streaming_response_writer.response_message
         protocol_writer
         id
         impl_menu_index
         ~data:(Ok Void))
  in
  { header_prefix
  ; data_len = Nat0.of_int_exn 0
  ; bin_writer
  ; protocol_writer
  ; id
  ; impl_menu_index
  ; description
  }
;;

let bin_writer t = t.bin_writer

(* This part of the message header is a constant, make it a literal to make the writing
   code slightly faster. *)
let stream_response_data_header_len = 4
let stream_response_data_header_as_int32 = 0x8a79l

let%test_unit "stream_response_* constants are correct" =
  let len =
    bin_writer_void_stream_response_data.write
      buffer
      ~pos:0
      (`Ok Void : void_stream_response_data)
  in
  assert (len = stream_response_data_header_len);
  assert (
    [%equal: int32]
      (Bigstring.unsafe_get_int32_t_le buffer ~pos:0)
      stream_response_data_header_as_int32)
;;

let bin_write_string_no_length buf ~pos str =
  let str_len = String.length str in
  (* Very low-level bin_prot stuff... *)
  Bin_prot.Common.assert_pos pos;
  let next = pos + str_len in
  Bin_prot.Common.check_next buf next;
  Bin_prot.Common.unsafe_blit_string_buf ~src_pos:0 str ~dst_pos:pos buf ~len:str_len;
  next
;;

(* The two following functions are used by the 3 variants exposed by this module. They
   serialize a [Response { id; data = Ok (`Ok data_len) }] value, taking care of writing
   the [Nat0.t] length prefix where appropriate.

   Bear in mind that there are two levels of length prefixes for stream response data
   message: one for the user data (under the `Ok, before the actual data), and one for the
   response data (under the .data field, after the Ok and before the `Ok).

   When eventually serialized, we get a sequence of bytes that can be broken down as:
   {v
   <Response> <id> <Ok> <len-of-stream-data> <`Ok> <len-of-actual-data> <actual-response>
  |                    |                    |     |                    |<- t.data_len -->|
  |<- header_prefix -->|                 -->|     |<--                 |                 |
  |                             stream_response_data_header_len        |                 |
  |                                         |<-------- stream_response_data_len -------->|
  |<------------------- bin_size_nat0_header ------------------------->|
  |<---------------- written by bin_write_nat0_header ---------------->|
   v}
*)
let bin_size_nat0_header { header_prefix; data_len; _ } =
  (* This is the length of [<`Ok> <len-of-actual-data>] *)
  let stream_response_data_nat0_len =
    stream_response_data_header_len + Nat0.bin_size_t data_len
  in
  (* This is the length of [<`Ok> <len-of-actual-data> <actual-response>] *)
  let stream_response_data_len =
    stream_response_data_nat0_len + (data_len : Nat0.t :> int)
  in
  (* This is the length of indicated by [bin_size_nat0_header] *)
  String.length header_prefix
  + Nat0.bin_size_t (Nat0.of_int_exn stream_response_data_len)
  + stream_response_data_nat0_len
;;

let bin_write_nat0_header buf ~pos { header_prefix; data_len; _ } =
  (* This wrote [<Response> <id> <Ok>] *)
  let pos = bin_write_string_no_length buf ~pos header_prefix in
  let stream_response_data_len =
    stream_response_data_header_len
    + Nat0.bin_size_t data_len
    + (data_len : Nat0.t :> int)
  in
  (* This wrote [<len-of-stream-data>] *)
  let pos = Nat0.bin_write_t buf ~pos (Nat0.of_int_exn stream_response_data_len) in
  let next = pos + 4 in
  Bin_prot.Common.check_next buf next;
  (* This wrote [<`Ok>] *)
  Bigstring.unsafe_set_int32_t_le buf ~pos stream_response_data_header_as_int32;
  (* This wrote [<len-of-actual-data>] *)
  Nat0.bin_write_t buf ~pos:next data_len
;;

let bin_writer_nat0_header : _ Bin_prot.Type_class.writer =
  { size = bin_size_nat0_header; write = bin_write_nat0_header }
;;

let bin_size_message (t, _) = bin_size_nat0_header t + (t.data_len : Nat0.t :> int)

let bin_write_message buf ~pos (t, data) =
  let pos = bin_write_nat0_header buf ~pos t in
  t.bin_writer.write buf ~pos data
;;

let bin_writer_message : _ Bin_prot.Type_class.writer =
  { size = bin_size_message; write = bin_write_message }
;;

let bin_size_message_as_string (t, _) =
  bin_size_nat0_header t + (t.data_len : Nat0.t :> int)
;;

let bin_write_message_as_string buf ~pos (t, str) =
  let pos = bin_write_nat0_header buf ~pos t in
  bin_write_string_no_length buf ~pos str
;;

let bin_writer_message_as_string : _ Bin_prot.Type_class.writer =
  { size = bin_size_message_as_string; write = bin_write_message_as_string }
;;

let flushed t = Protocol_writer.flushed t.protocol_writer

let write t data =
  t.data_len <- Nat0.of_int_exn (t.bin_writer.size data);
  Protocol_writer.Unsafe_for_cached_streaming_response_writer.send_bin_prot
    t.protocol_writer
    bin_writer_message
    (t, data)
  |> Protocol_writer.Response.handle_send_result
       t.protocol_writer
       t.id
       t.impl_menu_index
       t.description
       Streaming_update;
  ()
;;

let write_string t str =
  t.data_len <- Nat0.of_int_exn (String.length str);
  Protocol_writer.Unsafe_for_cached_streaming_response_writer.send_bin_prot
    t.protocol_writer
    bin_writer_message_as_string
    (t, str)
  |> Protocol_writer.Response.handle_send_result
       t.protocol_writer
       t.id
       t.impl_menu_index
       t.description
       Streaming_update;
  ()
;;

let write_expert t ~buf ~pos ~len =
  t.data_len <- Nat0.of_int_exn len;
  Protocol_writer.Unsafe_for_cached_streaming_response_writer.send_bin_prot_and_bigstring
    t.protocol_writer
    bin_writer_nat0_header
    t
    ~buf
    ~pos
    ~len
  |> Protocol_writer.Response.handle_send_result
       t.protocol_writer
       t.id
       t.impl_menu_index
       t.description
       Streaming_update;
  ()
;;

let schedule_write_expert ~(here : [%call_pos]) t ~buf ~pos ~len =
  t.data_len <- Nat0.of_int_exn len;
  let result =
    Protocol_writer.Unsafe_for_cached_streaming_response_writer
    .send_bin_prot_and_bigstring_non_copying
      t.protocol_writer
      bin_writer_nat0_header
      t
      ~buf
      ~pos
      ~len
  in
  Protocol_writer.Response.handle_send_result
    t.protocol_writer
    t.id
    t.impl_menu_index
    t.description
    Streaming_update
    result;
  match result with
  | Sent { result; bytes = _ } -> `Flushed { global = result }
  | Closed -> `Closed
  | Message_too_big too_big ->
    failwiths
      ~here
      "could not send as message too big"
      ([%globalize: Transport_intf.Send_result.message_too_big] too_big)
      [%sexp_of: Transport_intf.Send_result.message_too_big]
;;
