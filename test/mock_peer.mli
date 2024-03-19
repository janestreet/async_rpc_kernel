open! Core
open! Async
open! Import

(** A harness with a custom transport that allows:
    (a) closely observing the behaviour of the Async_rpc library
    (b) injecting messages for the library to read
    (c) observing the messages sent by the library
    (d) injecting various failures into the library *)

module Write_event : sig
  type t [@@deriving sexp_of]

  val bigstring_written : t -> Bigstring.t
end

module Event : sig
  type t =
    | Close_reader
    | Reader_waiting_on_scheduler
    | Close_writer
    | Wait_for_flushed of int
    | Wait_for_writer_ready
    | Write of Write_event.t
    | Tracing_event of Tracing_event.t
    | Close_started of Sexp.t
    | Close_finished
  [@@deriving sexp_of]
end

module Config : sig
  type t =
    { when_reader_waits : [ `Carry_on_until_end_of_batch | `Wait_immediately ]
    ; when_waiting_done : [ `Read_more | `Wait_for_mock_scheduling ]
    }
  [@@deriving sexp_of]
end

type t [@@deriving sexp_of]

(** Create a mock peer and connect to it. The handshake is done but nothing will be
    printed for it *)
val create_and_connect
  :  ?implementations:t Rpc.Implementations.t
  -> Config.t
  -> (t * Rpc.Connection.t) Deferred.t

(** Like [create_and_connect] but does not return the connection (most tests don’t need
    it) *)
val create_and_connect'
  :  ?implementations:t Rpc.Implementations.t
  -> Config.t
  -> t Deferred.t

(** Create a mock peer ready to connect *)
val create : ?time_source:Synchronous_time_source.t -> Config.t -> t

type handshake := [ `v3 ]

(** Attempt to connect to the mock peer *)
val connect
  :  ?implementations:t Rpc.Implementations.t
  -> ?send_handshake:handshake option
  -> t
  -> (Rpc.Connection.t, exn) result Deferred.t

(** Emit an event to the stream of events associated with [t] *)
val emit : t -> Event.t -> unit

(** Cause the connection’s reader to appear closed *)
val close_reader : t -> unit

(** The following functions enqueue messages for the Connection.t to read. Pass
    [~don't_read_yet:()] to batch with the next message. *)

val write_bigstring : ?don't_read_yet:unit -> t -> Bigstring.t -> unit
val write : ?don't_read_yet:unit -> t -> 'a Bin_prot.Writer.t -> 'a -> unit
val write_handshake : t -> handshake -> unit

val write_message
  :  ?don't_read_yet:unit
  -> t
  -> 'a Bin_prot.Writer.t
  -> 'a Protocol.Message.t
  -> unit

(** Continue the connection’s read loop when it is waiting *)
val continue_reader : t -> unit

(** The following enqueue parsers to help print out the messages emitted by the
    [Connection.t]. All enqueued parsers with [?later:None] will be run on messages the
    connection outputs and when that queue is empty, all enqueued parsers with [~later:()]
    will be moved to the first queue. This allows you to have rpc implementations [expect]
    messages for their responses and seperately [expect] other messages to follow those,
    calling [expect] before the rpc implementation.
*)

val expect : ?later:unit -> t -> 'a Bin_prot.Reader.t -> ('a -> Sexp.t) -> unit
val expect_message : ?later:unit -> t -> 'a Bin_prot.Reader.t -> ('a -> Sexp.t) -> unit

(** Enqueue the next return value to the Connection’s attempts to write. *)
val enqueue_send_result : t -> unit Rpc.Transport.Send_result.t -> unit

(** Cause all of the connections attempts to flush the writer up to flush N to be done. *)
val mark_flushed_up_to : t -> int -> unit

(** When a scheduled (i.e. non-copying) write is attempted, it must wait for the given
    ivar before the bigstring may be released. *)
val scheduled_writes_must_wait_for : t -> unit Deferred.t -> unit

(** [set_quiet t true] silences future messages and [false] the opposite. *)
val set_quiet : t -> bool -> unit

(** Set the function to be called on events *)
val set_on_emit : t -> (Event.t -> unit) -> unit
