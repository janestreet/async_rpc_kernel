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

let send_close_reason_if_supported t ~reason =
  match Set_once.get t.negotiated_protocol_version with
  | Some version when Version_dependent_feature.is_supported Close_reason ~version ->
    Some
      (Transport.Writer.send_bin_prot
         t.writer
         Protocol.Message.bin_writer_nat0_t
         (Close_reason reason))
  | Some (_ : int) | None -> None
;;

let response_message (type a) t (response : a Protocol.Response.t) : a Protocol.Message.t =
  let negotiated_protocol_version =
    Set_once.get_exn t.negotiated_protocol_version [%here]
  in
  (match response.data with
   | Ok (_ : a) -> response
   | Error rpc_error ->
     let error_implemented_in_protocol_version =
       Rpc_error.implemented_in_protocol_version rpc_error
     in
     (* We added [Unknown] in v3 to act as a catchall for future protocol errors. Before
        v3 we used [Uncaught_exn] as the catchall. *)
     if error_implemented_in_protocol_version <= negotiated_protocol_version
     then response
     else (
       let error_sexp = [%sexp_of: Protocol.Rpc_error.t] rpc_error in
       { response with
         data =
           Error
             (if negotiated_protocol_version >= 3
              then Unknown error_sexp
              else Uncaught_exn error_sexp)
       }))
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
