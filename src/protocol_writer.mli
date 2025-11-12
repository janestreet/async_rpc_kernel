open! Core
open Async_kernel

type t [@@deriving sexp_of]

val sexp_of_writer : t -> Sexp.t

val create_before_negotiation
  :  Transport.Writer.t
  -> tracing_events:(local_ Tracing_event.t -> unit) Bus.Read_write.t
  -> t

val set_negotiated_protocol_version : t -> int -> unit

module For_handshake : sig
  val send_handshake_header
    :  t
    -> Protocol_version_header.t
    -> (unit, Handshake_error.t) Result.t

  (** Returns [Ok ()] if we successfully sent, haven't negotiated a protocol version yet,
      or if the protocol version doesn't support sending the connection metadata. *)
  val send_connection_metadata_if_supported
    :  t
    -> Menu.t option
    -> identification:Bigstring.t option
    -> (unit, Handshake_error.t) Result.t
end

val send_heartbeat : t -> local_ unit Transport.Send_result.t

(** Returns [None] if we haven't negotiated a protocol version yet, or if the protocol
    version doesn't support sending the close reason. *)
val send_close_reason_if_supported
  :  t
  -> reason:Close_reason.Protocol.t
  -> local_ unit Transport.Send_result.t option

(** Returns [None] if we haven't negotiated a protocol version yet, or if the protocol
    version doesn't support sending the close started message. *)
val send_close_started_if_supported : t -> local_ unit Transport.Send_result.t option

val can_send : t -> bool
val bytes_to_write : t -> int
val bytes_written : t -> Int63.t
val flushed : t -> unit Deferred.t
val stopped : t -> unit Deferred.t
val close : t -> unit Deferred.t
val is_closed : t -> bool

module Query : sig
  val send
    :  t
    -> 'query Protocol.Query.Validated.t
    -> bin_writer_query:'query Bin_prot.Type_class.writer
    -> peer_menu:Menu.t option
    -> local_ unit Transport.Send_result.t

  val send_expert
    :  t
    -> unit Protocol.Query.Validated.t
    -> buf:Bigstring.t
    -> pos:int
    -> len:int
    -> send_bin_prot_and_bigstring:
         (Transport.Writer.t
          -> Protocol.Message.nat0_t Bin_prot.Type_class.writer
          -> Protocol.Message.nat0_t
          -> buf:Bigstring.t
          -> pos:int
          -> len:int
          -> local_ 'result Transport.Send_result.t)
    -> peer_menu:Menu.t option
    -> local_ 'result Transport.Send_result.t
end

module Response : sig
  val send
    :  t
    -> Protocol.Query_id.t
    -> Protocol.Impl_menu_index.t
    -> data:'response Rpc_result.t
    -> bin_writer_response:'response Bin_prot.Type_class.writer
    -> local_ unit Transport.Send_result.t

  val send_expert
    :  t
    -> Protocol.Query_id.t
    -> Protocol.Impl_menu_index.t
    -> buf:Bigstring.t
    -> pos:int
    -> len:int
    -> send_bin_prot_and_bigstring:
         (Transport.Writer.t
          -> Protocol.Message.nat0_t Bin_prot.Type_class.writer
          -> Protocol.Message.nat0_t
          -> buf:Bigstring.t
          -> pos:int
          -> len:int
          -> local_ 'result Transport.Send_result.t)
    -> local_ 'result Transport.Send_result.t

  val handle_send_result
    :  t
    -> local_ Protocol.Query_id.t
    -> local_ Protocol.Impl_menu_index.t
    -> local_ Description.t
    -> local_ Tracing_event.Sent_response_kind.t
    -> local_ 'a Transport_intf.Send_result.t
    -> unit
end

module Unsafe_for_cached_streaming_response_writer : sig
  val response_message
    :  t
    -> Protocol.Query_id.t
    -> Protocol.Impl_menu_index.t
    -> data:'response Rpc_result.t
    -> 'response Protocol.Message.maybe_needs_length

  val send_bin_prot
    :  t
    -> 'a Bin_prot.Type_class.writer
    -> 'a
    -> local_ unit Transport.Send_result.t

  val send_bin_prot_and_bigstring
    :  t
    -> 'a Bin_prot.Type_class.writer
    -> 'a
    -> buf:Bigstring.t
    -> pos:int
    -> len:int
    -> local_ unit Transport.Send_result.t

  val send_bin_prot_and_bigstring_non_copying
    :  t
    -> 'a Bin_prot.Type_class.writer
    -> 'a
    -> buf:Bigstring.t
    -> pos:int
    -> len:int
    -> local_ unit Deferred.t Transport.Send_result.t

  val transfer : t -> 'a Pipe.Reader.t -> ('a -> unit) -> unit Deferred.t
end
