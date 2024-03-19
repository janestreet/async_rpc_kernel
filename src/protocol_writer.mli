open! Core
open Async_kernel

type t [@@deriving sexp_of]

val sexp_of_writer : t -> Sexp.t
val create_before_negotiation : Transport.Writer.t -> t
val set_negotiated_protocol_version : t -> int -> unit

val send_query
  :  t
  -> 'query Protocol.Query.t
  -> bin_writer_query:'query Bin_prot.Type_class.writer
  -> unit Transport.Send_result.t

val send_expert_query
  :  t
  -> unit Protocol.Query.t
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

val send_response
  :  t
  -> 'response Protocol.Response.t
  -> bin_writer_response:'response Bin_prot.Type_class.writer
  -> unit Transport.Send_result.t

val send_expert_response
  :  t
  -> Protocol.Query_id.t
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

val send_heartbeat : t -> unit Transport.Send_result.t
val can_send : t -> bool
val bytes_to_write : t -> int
val bytes_written : t -> Int63.t
val flushed : t -> unit Deferred.t
val stopped : t -> unit Deferred.t
val close : t -> unit Deferred.t
val is_closed : t -> bool

module Unsafe_for_cached_bin_writer : sig
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
