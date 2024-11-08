open Core
open Async_kernel

type 'a message_handler = Bigstring.t -> pos:int -> len:int -> 'a

module Handler_result = struct
  (** Result of an [on_message] callback.  We split the [Continue] and [Wait _] cases to
      make it clear that [Continue] is the expected case.  The implementation should be
      optimized for this case. *)
  type 'a t =
    | Stop of 'a
    | Continue
    | Wait of unit Deferred.t
end

module type Reader = sig
  type t [@@deriving sexp_of]

  val close : t -> unit Deferred.t
  val is_closed : t -> bool
  val bytes_read : t -> Int63.t

  (** Start reading incoming messages and pass them to [on_message], until it returns
      [Stop _].

      [on_end_of_batch] is called after processing a batch of messages, before waiting for
      the file descriptor to become readable again. *)
  val read_forever
    :  t
    -> on_message:(Bigstring.t -> pos:int -> len:int -> 'a Handler_result.t)
    -> on_end_of_batch:(unit -> unit)
    -> ('a, [ `Eof | `Closed ]) Result.t Deferred.t
end

module Send_result = struct
  type message_too_big =
    { size : int
    ; max_message_size : int
    }
  [@@deriving bin_io, compare, globalize, sexp]

  type 'a t =
    | Sent of
        { global_ result : 'a
        ; bytes : int
        (** Bytes should equal the size of the bin_prot rpc message and data. The total
            bytes written on the network in the standard protocol (which has 8-bytes sizes
            before each frame) will be [sum([8 + x.bytes for each send result x])]. Other
            framing protocols or encryption (e.g. rpc over kerberos) may write more or
            less bytes. *)
        }
    | Closed
    | Message_too_big of message_too_big
  [@@deriving compare, globalize, sexp_of]
end

module type Writer = sig
  type t [@@deriving sexp_of]

  val close : t -> unit Deferred.t
  val is_closed : t -> bool
  val monitor : t -> Monitor.t
  val bytes_to_write : t -> int
  val bytes_written : t -> Int63.t

  (** Becomes determined when it is no longer possible to send message using this writer,
      for instance when the writer is closed or the consumer leaves.

      The result of this function is cached by [Rpc.Transport] *)
  val stopped : t -> unit Deferred.t

  (** [flushed t] returns a deferred that must become determined when all prior sent
      messages have either been flushed to the underlying stream transport or have been
      dropped because the underlying transport has closed.

      It must be OK to call [flushed t] after [t] has been closed. *)
  val flushed : t -> unit Deferred.t

  (** [ready_to_write t] becomes determined when it is a good time to send messages
      again. Async RPC calls this function after sending a batch of messages, to avoid
      flooding the transport.

      Using [let ready_to_write = flushed] is an acceptable implementation. *)
  val ready_to_write : t -> unit Deferred.t

  (** All the following functions send exactly one message. *)

  val send_bin_prot
    :  t
    -> 'a Bin_prot.Type_class.writer
    -> 'a
    -> local_ unit Send_result.t

  val send_bin_prot_and_bigstring
    :  t
    -> 'a Bin_prot.Type_class.writer
    -> 'a
    -> buf:Bigstring.t
    -> pos:int
    -> len:int
    -> local_ unit Send_result.t

  (** Same as [send_bin_prot_and_bigstring] but the bigstring can't be modified until the
      returned deferred becomes determined.  This can be used to avoid copying the
      bigstring. *)
  val send_bin_prot_and_bigstring_non_copying
    :  t
    -> 'a Bin_prot.Type_class.writer
    -> 'a
    -> buf:Bigstring.t
    -> pos:int
    -> len:int
    -> local_ unit Deferred.t Send_result.t
end
