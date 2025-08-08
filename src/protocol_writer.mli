open! Core
open Async_kernel

type t [@@deriving sexp_of]

val sexp_of_writer : t -> Sexp.t

val create_before_negotiation
  :  Transport.Writer.t
  -> tracing_events:(Tracing_event.t -> unit) Bus.Read_write.t
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

val send_heartbeat : t -> unit Transport.Send_result.t

(** Returns [None] if we haven't negotiated a protocol version yet, or if the protocol
    version doesn't support sending the close reason. *)
val send_close_reason_if_supported
  :  t
  -> reason:Info.t
  -> unit Transport.Send_result.t option

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
    -> 'query Protocol.Query.V3.t
    -> bin_writer_query:'query Bin_prot.Type_class.writer
    -> unit Transport.Send_result.t

  val send_expert
    :  t
    -> unit Protocol.Query.V3.t
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
          -> 'result Transport.Send_result.t)
    -> 'result Transport.Send_result.t
end

module Response : sig
  val send
    :  t
    -> Protocol.Query_id.t
    -> Protocol.Impl_menu_index.t
    -> data:'response Rpc_result.t
    -> bin_writer_response:'response Bin_prot.Type_class.writer
    -> unit Transport.Send_result.t

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
          -> 'result Transport.Send_result.t)
    -> 'result Transport.Send_result.t

  val handle_send_result
    :  t
    -> Protocol.Query_id.t
    -> Protocol.Impl_menu_index.t
    -> Description.t
    -> Tracing_event.Sent_response_kind.t
    -> 'a Transport_intf.Send_result.t
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
    -> unit Transport.Send_result.t

  val send_bin_prot_and_bigstring
    :  t
    -> 'a Bin_prot.Type_class.writer
    -> 'a
    -> buf:Bigstring.t
    -> pos:int
    -> len:int
    -> unit Transport.Send_result.t

  val send_bin_prot_and_bigstring_non_copying
    :  t
    -> 'a Bin_prot.Type_class.writer
    -> 'a
    -> buf:Bigstring.t
    -> pos:int
    -> len:int
    -> unit Deferred.t Transport.Send_result.t

  val transfer : t -> 'a Pipe.Reader.t -> ('a -> unit) -> unit Deferred.t
end
