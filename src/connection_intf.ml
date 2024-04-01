open Core
open Async_kernel

(* The reason for defining this module type explicitly is so that we can internally keep
   track of what is and isn't exposed. *)
module type S = sig
  type t [@@deriving sexp_of]

  module Heartbeat_config : sig
    type t [@@deriving sexp, bin_io]

    (** Each side of the connection has its own heartbeat config. It sends a heartbeat
        every [send_every]. If it doesn't receive any messages for [timeout], whether it's
        a heartbeat or not, it drops the connection. It only checks whether [timeout] has
        elapsed when it sends heartbeats, so effectively [timeout] is rounded up to the
        nearest multiple of [send_every]. *)
    val create : ?timeout:Time_ns.Span.t -> ?send_every:Time_ns.Span.t -> unit -> t

    val timeout : t -> Time_ns.Span.t
    val send_every : t -> Time_ns.Span.t
  end

  module Client_implementations : sig
    type connection := t

    type t =
      | T :
          { connection_state : connection -> 's
          ; implementations : 's Implementations.t
          }
          -> t

    val null : unit -> t
  end

  (** Initiate an Rpc connection on the given transport.  [implementations] should be the
      bag of implementations that the calling side implements; it defaults to
      [Implementations.null] (i.e., "I implement no RPCs").

      [protocol_version_headers] should contain the pre-shared protocol version headers,
      if applicable. The client and server must agree on whether these were shared and, if
      so, what they contained, or there may be a protocol error.

      [connection_state] will be called once, before [create]'s result is determined, on
      the same connection that [create] returns.  Its output will be provided to the
      [implementations] when queries arrive.

      WARNING: If specifying a custom [heartbeat_config], make sure that both ends of the
      Rpc connection use compatible settings for timeout and send frequency. Otherwise,
      your Rpc connections might close unexpectedly.

      [max_metadata_size] will limit how many bytes of metadata this peer can send along
      with each query. It defaults to 1k. User-provided metadata exceeding that size will
      be truncated.
      WARNING: setting this value too high allows this connection to send large amounts of
      data to the callee, unnoticed, which can severely degrade performance.

      [description] can be used to give some extra information about the connection, which
      will then show up in error messages and the connection's sexp. If you have lots of
      connections in your program, this can be useful for distinguishing them.

      [time_source] can be given to define the time_source for which the heartbeating
      events will be scheduled. Defaults to wall-clock.

      [identification] can be used to send an additional information to the peer. This is
      intended to be used for identifying the identity of the /process/ as opposed to the
      identity of the user. We use a bigstring to leave the option for clients to
      interpret as structured data of their choosing. *)
  val create
    :  ?implementations:'s Implementations.t
    -> ?protocol_version_headers:Protocol_version_header.Pair.t
    -> connection_state:(t -> 's)
    -> ?handshake_timeout:Time_ns.Span.t
    -> ?heartbeat_config:Heartbeat_config.t
    -> ?max_metadata_size:Byte_units.t
    -> ?description:Info.t
    -> ?time_source:Synchronous_time_source.t
    -> ?identification:Bigstring.t
    -> Transport.t
    -> (t, Exn.t) Result.t Deferred.t

  (** As of Feb 2017, the RPC protocol started to contain a magic number so that one can
      identify RPC communication.  The bool returned by [contains_magic_prefix] says
      whether this magic number was observed. *)
  val contains_magic_prefix : bool Bin_prot.Type_class.reader

  val description : t -> Info.t

  (** After [add_heartbeat_callback t f], [f ()] will be called after every subsequent
      heartbeat received by [t]. *)
  val add_heartbeat_callback : t -> (unit -> unit) -> unit

  (** Changes the heartbeat timeout and restarts the timer by setting [last_seen_alive] to
      the current time. *)
  val reset_heartbeat_timeout : t -> Time_ns.Span.t -> unit

  (** The last time either any message has been received or [reset_heartbeat_timeout] was
      called. *)
  val last_seen_alive : t -> Time_ns.t

  (** [close] starts closing the connection's transport, and returns a deferred that
      becomes determined when its close completes.  It is ok to call [close] multiple
      times on the same [t]; calls subsequent to the initial call will have no effect, but
      will return the same deferred as the original call.

      Before closing the underlying transport's writer, [close] waits for all streaming
      responses to be [Pipe.upstream_flushed] with a timeout of
      [streaming_responses_flush_timeout].

      The [reason] for closing the connection will be passed to callers of [close_reason].
  *)
  val close
    :  ?streaming_responses_flush_timeout:Time_ns.Span.t (* default: 5 seconds *)
    -> ?reason:Info.t
    -> t
    -> unit Deferred.t

  (** [close_finished] becomes determined after the close of the connection's transport
      completes, i.e. the same deferred that [close] returns.  [close_finished] differs
      from [close] in that it does not have the side effect of initiating a close. *)
  val close_finished : t -> unit Deferred.t

  (** [close_reason ~on_close t] becomes determined when close starts or finishes
      based on [on_close], but additionally returns the reason that the connection was
      closed. *)
  val close_reason : t -> on_close:[ `started | `finished ] -> Info.t Deferred.t

  (** [is_closed t] returns [true] iff [close t] has been called.  [close] may be called
      internally upon errors or timeouts. *)
  val is_closed : t -> bool

  (** [bytes_to_write] and [flushed] just call the similarly named function on the
      [Transport.Writer.t] within a connection. *)
  val bytes_to_write : t -> int

  (** [bytes_written] just calls the similarly named functions on the [Transport.Writer.t]
      within a connection. *)
  val bytes_written : t -> Int63.t

  (** [bytes_read] just calls the similarly named function on the [Transport.Reader.t]
      within a connection. *)
  val bytes_read : t -> Int63.t

  val flushed : t -> unit Deferred.t

  (** Peer menu will become determined before any other messages are received. The menu is
      sent automatically on creation of a connection. If the peer is using an older
      version, the value is immediately determined to be [None]. If the connection is
      closed before the menu is received, an error is returned.

      It is expected that one will call {!Versioned_rpc.Connection_with_menu.create}
      instead of this function and that will request the menu via rpc if it gets [None].
  *)
  val peer_menu : t -> Menu.t option Or_error.t Deferred.t

  (** Like {!peer_menu} but returns an rpc result  *)
  val peer_menu' : t -> Menu.t option Rpc_result.t Deferred.t

  val my_menu : t -> Menu.t option

  (** Peer identification will become determined before any other messages are received.
      If the peer is using an older version, the peer id is immediately determined to be
      [None]. If the connection is closed before the menu is received, [None] is returned.
  *)
  val peer_identification : t -> Bigstring.t option Deferred.t

  (** [with_close] tries to create a [t] using the given transport.  If a handshake error
      is the result, it calls [on_handshake_error], for which the default behavior is to
      raise an exception.  If no error results, [dispatch_queries] is called on [t].

      After [dispatch_queries] returns, if [server] is None, the [t] will be closed and
      the deferred returned by [dispatch_queries] will be determined immediately.
      Otherwise, we'll wait until the other side closes the connection and then close [t]
      and determine the deferred returned by [dispatch_queries].

      When the deferred returned by [with_close] becomes determined, [Transport.close] has
      finished.

      NOTE: Because this connection is closed when the [Deferred.t] returned by
      [dispatch_queries] is determined, you should be careful when using this with
      [Pipe_rpc].  For example, simply returning the pipe when you get it will close the
      pipe immediately.  You should instead either use the pipe inside [dispatch_queries]
      and not determine its result until you are done with the pipe, or use a different
      function like [create]. *)
  val with_close
    :  ?implementations:'s Implementations.t
    -> ?protocol_version_headers:Protocol_version_header.Pair.t
    -> ?handshake_timeout:Time_ns.Span.t
    -> ?heartbeat_config:Heartbeat_config.t
    -> ?description:Info.t
    -> ?time_source:Synchronous_time_source.t
    -> connection_state:(t -> 's)
    -> Transport.t
    -> dispatch_queries:(t -> 'a Deferred.t)
    -> on_handshake_error:[ `Raise | `Call of Exn.t -> 'a Deferred.t ]
    -> 'a Deferred.t

  (** Runs [with_close] but dispatches no queries. The implementations are required
      because this function doesn't let you dispatch any queries (i.e., act as a client),
      it would be pointless to call it if you didn't want to act as a server.*)
  val server_with_close
    :  ?handshake_timeout:Time_ns.Span.t
    -> ?heartbeat_config:Heartbeat_config.t
    -> ?description:Info.t
    -> ?time_source:Synchronous_time_source.t
    -> Transport.t
    -> implementations:'s Implementations.t
    -> connection_state:(t -> 's)
    -> on_handshake_error:[ `Raise | `Ignore | `Call of Exn.t -> unit Deferred.t ]
    -> unit Deferred.t
end

module type S_private = sig
  open Protocol
  include S

  (* Internally, we use a couple of extra functions on connections that aren't exposed to
     users. *)

  val compute_metadata : t -> Description.t -> Query_id.t -> Rpc_metadata.t option

  type response_with_determinable_status =
    | Pipe_eof
    | Expert_indeterminate
    | Determinable :
        'a Rpc_result.t * 'a Implementation.F.error_mode
        -> response_with_determinable_status

  type response_handler_action =
    | Keep
    | Wait of unit Deferred.t
    | Remove of (response_with_determinable_status, Rpc_error.t Gel.t) result
    | Expert_remove_and_wait of unit Deferred.t

  type response_handler =
    Nat0.t Response.t
    -> read_buffer:Bigstring.t
    -> read_buffer_pos_ref:int ref
    -> response_handler_action

  val sexp_of_t_hum_writer : t -> Sexp.t

  module Dispatch_error : sig
    type t =
      | Closed
      | Message_too_big of Transport.Send_result.message_too_big
    [@@deriving sexp_of]
  end

  val dispatch
    :  t
    -> kind:Tracing_event.Sent_response_kind.t Tracing_event.Kind.t
    -> response_handler:response_handler option
    -> bin_writer_query:'a Bin_prot.Type_class.writer
    -> query:'a Query.t
    -> (unit, Dispatch_error.t) Result.t

  val dispatch_bigstring
    :  t
    -> tag:Rpc_tag.t
    -> version:int
    -> metadata:unit
    -> Bigstring.t
    -> pos:int
    -> len:int
    -> response_handler:response_handler option
    -> (unit, Dispatch_error.t) Result.t

  val schedule_dispatch_bigstring
    :  t
    -> tag:Rpc_tag.t
    -> version:int
    -> metadata:unit
    -> Bigstring.t
    -> pos:int
    -> len:int
    -> response_handler:response_handler option
    -> (unit Deferred.t, Dispatch_error.t) Result.t

  val schedule_dispatch_bigstring_with_metadata
    :  t
    -> tag:Rpc_tag.t
    -> version:int
    -> metadata:string option
    -> Bigstring.t
    -> pos:int
    -> len:int
    -> response_handler:response_handler option
    -> (unit Deferred.t, Dispatch_error.t) Result.t

  val default_handshake_timeout : Time_ns.Span.t

  (** Allows getting information from the RPC that may be used for tracing or metrics. The
      interface is not yet stable. *)
  val events : t -> (Tracing_event.t -> unit) Bus.Read_only.t

  (** The header that would be sent at the beginning of a connection. This can be used to
      pre-share this part of the handshake (see the [protocol_version_headers] argument to
      [create]). *)
  val default_handshake_header : Protocol_version_header.t

  (** Allows some extra information to be passed between clients and servers (e.g. for
      tracing). The [when_sending] function is called to compute the metadata that is sent
      along with rpc queries. It is called in the same async context as the dispatch
      function. The [on_receive] function is called before an rpc implementation to modify
      the execution context for that implementation. Metadata is not sent if the server is
      running an old version of the Async_rpc protocol. Handlers should not change the
      monitor on the execution context (this will have no effect on where errors are sent
      for rpc implementations).

      The passed [query_id] may be used to correlate with a listener on the {!events} bus. *)
  val set_metadata_hooks
    :  t
    -> when_sending:(Description.t -> query_id:Int63.t -> Rpc_metadata.t option)
    -> on_receive:
         (Description.t
          -> query_id:Int63.t
          -> Rpc_metadata.t option
          -> Execution_context.t
          -> Execution_context.t)
    -> [ `Ok | `Already_set ]

  (** True if future calls to [set_metadata_hooks] will return [`Already_set]. *)
  val have_metadata_hooks_been_set : t -> bool

  module For_testing : sig
    module Header : sig
      type t [@@deriving bin_io, sexp_of]

      val v1 : t
      val v2 : t
      val v3 : t
    end

    val with_async_execution_context : context:Header.t -> f:(unit -> 'a) -> 'a
  end
end
