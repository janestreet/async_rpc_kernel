open! Core

type t =
  { negotiated_protocol_version : int Set_once.t
  ; writer : Transport.Writer.t
  }
[@@deriving sexp_of]

let sexp_of_writer t = [%sexp_of: Transport.Writer.t] t.writer

let create_before_negotiation writer =
  { negotiated_protocol_version = Set_once.create (); writer }
;;

let set_negotiated_protocol_version t negotiated_protocol_version =
  Set_once.set_exn t.negotiated_protocol_version [%here] negotiated_protocol_version
;;

let query_message t query : _ Protocol.Message.t =
  match Set_once.get_exn t.negotiated_protocol_version [%here] with
  | 1 -> Query_v1 (Protocol.Query.to_v1 query)
  | _ -> Query query
;;

let send_query t query ~bin_writer_query =
  let message = query_message t query in
  Transport.Writer.send_bin_prot
    t.writer
    (Protocol.Message.bin_writer_maybe_needs_length
       (Writer_with_length.of_writer bin_writer_query))
    message
;;

let send_expert_query t query ~buf ~pos ~len ~send_bin_prot_and_bigstring =
  let header = query_message t { query with data = Nat0.of_int_exn len } in
  send_bin_prot_and_bigstring
    t.writer
    Protocol.Message.bin_writer_nat0_t
    header
    ~buf
    ~pos
    ~len
;;

let send_heartbeat t =
  Transport.Writer.send_bin_prot t.writer Protocol.Message.bin_writer_nat0_t Heartbeat
;;

let response_message (type a) t (response : a Protocol.Response.t) : a Protocol.Message.t =
  let negotiated_protocol_version =
    Set_once.get_exn t.negotiated_protocol_version [%here]
  in
  (match response.data with
   | Ok (_ : a) -> response
   | Error
       (( Bin_io_exn _ | Connection_closed | Write_error _ | Uncaught_exn _
        | Unimplemented_rpc (_, _)
        | Unknown_query_id _ ) as _v1_error) -> response
   | Error ((Authorization_failure _ | Message_too_big _ | Unknown _) as v3_error) ->
     if negotiated_protocol_version < 3
     then
       { response with
         data = Error (Uncaught_exn ([%sexp_of: Protocol.Rpc_error.t] v3_error))
       }
     else response)
  |> Response
;;

let send_response t response ~bin_writer_response =
  let message = response_message t response in
  Transport.Writer.send_bin_prot
    t.writer
    (Protocol.Message.bin_writer_maybe_needs_length
       (Writer_with_length.of_writer bin_writer_response))
    message
;;

let send_expert_response t query_id ~buf ~pos ~len ~send_bin_prot_and_bigstring =
  let header = response_message t { id = query_id; data = Ok (Nat0.of_int_exn len) } in
  send_bin_prot_and_bigstring
    t.writer
    Protocol.Message.bin_writer_nat0_t
    header
    ~buf
    ~pos
    ~len
;;

let of_writer f t = f t.writer
let can_send = of_writer Transport.Writer.can_send
let bytes_to_write = of_writer Transport.Writer.bytes_to_write
let bytes_written = of_writer Transport.Writer.bytes_written
let flushed = of_writer Transport.Writer.flushed
let stopped = of_writer Transport.Writer.stopped
let close = of_writer Transport.Writer.close
let is_closed = of_writer Transport.Writer.is_closed

module Unsafe_for_cached_bin_writer = struct
  let send_bin_prot t bin_writer a = Transport.Writer.send_bin_prot t.writer bin_writer a

  let send_bin_prot_and_bigstring t bin_writer a ~buf ~pos ~len =
    Transport.Writer.send_bin_prot_and_bigstring t.writer bin_writer a ~buf ~pos ~len
  ;;

  let send_bin_prot_and_bigstring_non_copying t bin_writer a ~buf ~pos ~len =
    Transport.Writer.send_bin_prot_and_bigstring_non_copying
      t.writer
      bin_writer
      a
      ~buf
      ~pos
      ~len
  ;;

  let transfer t pipe_reader f = Transport.Writer.transfer t.writer pipe_reader f
end
