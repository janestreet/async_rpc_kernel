(** A library for building asynchronous RPC-style protocols.

    The approach here is to have a separate representation of the server-side
    implementation of an RPC (An [Implementation.t]) and the interface that it exports
    (either an [Rpc.t], a [State_rpc.t] or a [Pipe_rpc.t], but we'll refer to them
    generically as RPC interfaces). A server builds the [Implementation.t] out of an RPC
    interface and a function for implementing the RPC, while the client dispatches a
    request using the same RPC interface.

    The [Implementation.t] hides the type of the query and the response, whereas the
    [Rpc.t] is polymorphic in the query and response type. This allows you to build a
    [Implementations.t] out of a list of [Implementation.t]s.

    Each RPC also comes with a version number. This is meant to allow support of multiple
    different versions of what is essentially the same RPC. You can think of it as an
    extension to the name of the RPC, and in fact, each RPC is uniquely identified by its
    (name, version) pair. RPCs with the same name but different versions should implement
    similar functionality.

    {3 Ordering}
    A single [Async_rpc] connection is an ordered stream of messages, where a message is
    something like a query or a response. Functions that directly write a message, e.g.
    dispatch functions, [Direct_stream_writer.write], etc., will synchronously enqueue
    that message to be sent. Received messages will be processed in order, so functions
    that receive a message, like RPC implementations, or the callback on
    [Pipe_rpc.dispatch_iter], will be invoked as soon as the message is received.

    This means that if careful, assumptions can be made about the ordering of messages,
    e.g.:
    - Multiple dispatches of RPCs will be received by the other end of the connection in
      the order they were sent
    - Once a [Direct_stream_writer] has been started (see
      {!Pipe_rpc.Direct_stream_writer.started} for details on what that means), values
      written to it will be immediately added to the message stream. This means if you
      write a value to a started writer, and the other end of the connection used
      [dispatch_iter] to dispatch the streaming RPC, the callback is guaranteed to be
      invoked on that message before any other message on the same connection produced
      after the call to [Direct_stream_writer.write], e.g. responding to RPC response, or
      an RPC dispatch.

    Note that once asynchrony is introduced, e.g. with non-eager RPC implementations,
    using [Pipe.t] to dispatch or implement streaming RPCs, or using functions like
    [lift_deferred] or [with_authorization_deferred], it becomes much harder to make
    assertions about the relative ordering of messages. *)

open! Core
open! Async_kernel

module Description : sig
  type t = Description.t =
    { name : string [@globalized]
    ; version : int
    }
  [@@deriving compare, equal, hash, sexp_of, globalize]

  include Comparable.S_plain with type t := t
  include Hashable.S_plain with type t := t

  val summarize : t list -> Int.Set.t String.Map.t
  val of_alist : (string * int) list -> t list
  val to_alist : t list -> (string * int) list

  module Stable : sig
    module V1 : sig
      type nonrec t = t [@@deriving compare, equal, sexp, bin_io, hash, stable_witness]
    end
  end
end

(** When your implementation raises an exception, that exception might happen before a
    value is returned or after a value is returned. The latter kind of exception is what
    the [~rest] flag to [Monitor.try_with] is dealing with. *)
module On_exception = On_exception

(** A ['connection_state Implementation.t] is something that knows how to respond to one
    query, given a ['connection_state]. That is, you can create a
    ['connection_state Implementation.t] by providing a function which takes a query *and*
    a ['connection_state] and provides a response.

    The reason for this is that RPCs often do something like look something up in a master
    structure. This way, [Implementation.t]s can be created without having the master
    structure in your hands. *)
module Implementation : sig
  type 'connection_state t = 'connection_state Implementation.t [@@deriving sexp_of]

  val description : _ t -> Description.t
  val digests : _ t -> Rpc_shapes.Just_digests.t
  val shapes : _ t -> Rpc_shapes.t

  (** We may want to use an ['a t] implementation (perhaps provided by someone else) in a
      ['b t] context. We can do this as long as we can map our state into the state
      expected by the original implementer.

      Note [f] is called on every RPC rather than once on the initial connection state. *)
  val lift : 'a t -> f:('b -> 'a) -> 'b t

  (** Like [lift] above, but allows for the possibility of the state mapping failing. If
      [Error] is returned, then the peer will receive that error. *)
  val try_lift : 'a t -> f:('b -> 'a Or_error.t) -> 'b t

  (** Similar to [lift], but useful if you want to do asynchronous work to map connection
      state. Like [lift], [f] will be called on every RPC. This can make RPC
      implementations less efficient if they were previously relying on functions like
      [Rpc.implement'] to avoid extra async work to send responses. This can also allow
      blocking implementations to run in a different order from the order that queries
      arrive on a connection. *)
  val lift_deferred : 'a t -> f:('b -> 'a Deferred.t) -> 'b t

  val try_lift_deferred : 'a t -> f:('b -> 'a Or_error.t Deferred.t) -> 'b t
  val with_authorization : 'a t -> f:('b -> 'a Or_not_authorized.t) -> 'b t

  val with_authorization_deferred
    :  'a t
    -> f:('b -> 'a Or_not_authorized.t Deferred.t)
    -> 'b t

  val update_on_exception : 'a t -> f:(On_exception.t option -> On_exception.t) -> 'a t
end

(** A ['connection_state Implementations.t] is something that knows how to respond to many
    different queries. It is conceptually a package of
    ['connection_state Implementation.t]s. *)
module Implementations : sig
  type 'connection_state t = 'connection_state Implementations.t

  (** a server that can handle no queries *)
  val null : unit -> 'connection_state t

  (** Calls [Implementation.lift] on all implementations in [t]. *)
  val lift : 'a t -> f:('b -> 'a) -> 'b t

  (** Apply a function to the implementations in [t]. *)
  val map_implementations
    :  'a t
    -> f:('a Implementation.t list -> 'a Implementation.t list)
    -> ('a t, [ `Duplicate_implementations of Description.t list ]) Result.t

  type 'connection_state on_unknown_rpc =
    [ `Raise
    | `Continue
    | `Close_connection (** used to be the behavior of [`Ignore] *)
    | `Call of
      'connection_state
      -> rpc_tag:string
      -> version:int
      -> [ `Close_connection | `Continue ]
      (** [rpc_tag] and [version] are the name and version of the unknown rpc *)
    ]

  (** [create ~implementations ~on_unknown_rpc ~on_exception] creates a server capable of
      responding to the rpcs implemented in the implementation list. Be careful about
      setting [on_unknown_rpc] to [`Raise] because other programs may mistakenly connect
      to this one causing it to crash. *)
  val create
    :  implementations:'connection_state Implementation.t list
    -> on_unknown_rpc:'connection_state on_unknown_rpc
    -> on_exception:On_exception.t
    -> ( 'connection_state t
         , [ `Duplicate_implementations of Description.t list ] )
         Result.t

  val create_exn
    :  implementations:'connection_state Implementation.t list
    -> on_unknown_rpc:
         [ `Raise
         | `Continue
         | `Close_connection (** used to be the behavior of [`Ignore] *)
         | `Call of
           'connection_state
           -> rpc_tag:string
           -> version:int
           -> [ `Close_connection | `Continue ]
         ]
    -> on_exception:On_exception.t
    -> 'connection_state t

  val add
    :  'connection_state t
    -> 'connection_state Implementation.t
    -> 'connection_state t Or_error.t

  val add_exn
    :  'connection_state t
    -> 'connection_state Implementation.t
    -> 'connection_state t

  val descriptions : _ t -> Description.t list

  (** Low-level, untyped access to queries. Regular users should ignore this. *)
  module Expert : sig
    (** See [Rpc.Expert.Responder] for how to use this. *)
    module Responder : sig
      type t
    end

    (** Same as [create_exn], except for the additional [`Expert] variant. *)
    val create_exn
      :  implementations:'connection_state Implementation.t list
      -> on_unknown_rpc:
           [ `Raise
           | `Continue
           | `Close_connection (** used to be the behavior of [`Ignore] *)
           | `Call of
             'connection_state
             -> rpc_tag:string
             -> version:int
             -> [ `Close_connection | `Continue ]
           | `Expert of
             'connection_state
             -> rpc_tag:string
             -> version:int
             -> metadata:Rpc_metadata.V1.t option
             -> Responder.t
             -> Bigstring.t
             -> pos:int
             -> len:int
             -> unit Deferred.t
             (** The [Deferred.t] the function returns is only used to determine when it
                 is safe to overwrite the supplied [Bigstring.t], so it is *not* necessary
                 to completely finish handling the query before it is filled in. In
                 particular, if you don't intend to read from the [Bigstring.t] after the
                 function returns, you can return [Deferred.unit]. *)
           ]
      -> on_exception:On_exception.t
      -> 'connection_state t
  end

  module Private = Implementations.Private
end

module Transport = Transport
module Connection : Connection_intf.S with type t = Connection.t
module How_to_recognise_errors = How_to_recognise_errors

module Rpc : sig
  type ('query, 'response) t

  val create
    :  name:string
    -> version:int
    -> bin_query:'a Bin_prot.Type_class.t
    -> bin_response:'response Bin_prot.Type_class.t
    -> include_in_error_count:'response How_to_recognise_errors.t
    -> ('a, 'response) t

  (** the same values as were passed to create. *)
  val name : (_, _) t -> string

  val version : (_, _) t -> int
  val description : (_, _) t -> Description.t
  val query_type_id : ('query, _) t -> 'query Type_equal.Id.t
  val response_type_id : (_, 'response) t -> 'response Type_equal.Id.t
  val bin_query : ('query, _) t -> 'query Bin_prot.Type_class.t
  val bin_response : (_, 'response) t -> 'response Bin_prot.Type_class.t
  val shapes : (_, _) t -> Rpc_shapes.t

  (** If the function that implements the RPC raises, the implementer does not see the
      exception. Instead, it is sent as an error to the caller of the RPC, i.e. the
      process that called [dispatch] or one of its alternatives. *)
  val implement
    :  ?here:Stdlib.Lexing.position
    -> ?on_exception:On_exception.t
    -> ('query, 'response) t
    -> ('connection_state -> 'query -> 'response Deferred.t)
    -> 'connection_state Implementation.t

  val implement_with_auth
    :  ?here:Stdlib.Lexing.position
    -> ?on_exception:On_exception.t
    -> ('query, 'response) t
    -> ('connection_state -> 'query -> 'response Or_not_authorized.t Deferred.t)
    -> 'connection_state Implementation.t

  (** [implement'] is different from [implement] in that:

      1. ['response] is immediately serialized and scheduled for delivery to the RPC
         dispatcher.

      2. Less allocation happens, as none of the Async-related machinery is necessary.

      [implement] also tries to do 1 when possible, but it is guaranteed to happen with
      [implement']. *)
  val implement'
    :  ?here:Stdlib.Lexing.position
    -> ?on_exception:On_exception.t
    -> ('query, 'response) t
    -> ('connection_state -> 'query -> 'response)
    -> 'connection_state Implementation.t

  val implement_with_auth'
    :  ?here:Stdlib.Lexing.position
    -> ?on_exception:On_exception.t
    -> ('query, 'response) t
    -> ('connection_state -> 'query -> 'response Or_not_authorized.t)
    -> 'connection_state Implementation.t

  (** [dispatch'] exposes [Rpc_result.t] as output. Passing it through
      [rpc_result_to_or_error] gives you the same result as [dispatch] *)
  val dispatch'
    :  ('query, 'response) t
    -> Connection.t
    -> 'query
    -> 'response Rpc_result.t Deferred.t

  val rpc_result_to_or_error
    :  ('query, 'response) t
    -> Connection.t
    -> 'response Rpc_result.t
    -> 'response Or_error.t

  val dispatch
    :  ('query, 'response) t
    -> Connection.t
    -> 'query
    -> 'response Or_error.t Deferred.t

  val dispatch_exn
    :  ('query, 'response) t
    -> Connection.t
    -> 'query
    -> 'response Deferred.t

  module Expert : sig
    module Responder : sig
      type t = Implementations.Expert.Responder.t

      (** As in [Writer], after calling [schedule], you should not overwrite the
          [Bigstring.t] passed in until the responder is flushed. *)
      val schedule
        :  t
        -> Bigstring.t
        -> pos:int
        -> len:int
        -> [ `Flushed of unit Deferred.t | `Connection_closed ]

      (** On the other hand, these are written immediately. *)
      val write_bigstring : t -> Bigstring.t -> pos:int -> len:int -> unit

      val write_bin_prot : t -> 'a Bin_prot.Type_class.writer -> 'a -> unit
      val write_error : t -> Error.t -> unit
    end

    (** This just schedules a write, so the [Bigstring.t] should not be overwritten until
        the flushed [Deferred.t] is determined. The return value of [handle_response] has
        the same meaning as in the function argument of [Implementations.Expert.create].

        [schedule_dispatch] will raise if the length of the RPC message (i.e. [len] plus
        the message header) is longer than the underlying transport's max message size. *)
    val schedule_dispatch
      :  Connection.t
      -> rpc_tag:string
      -> version:int
      -> Bigstring.t
      -> pos:int
      -> len:int
      -> handle_response:(Bigstring.t -> pos:int -> len:int -> unit Deferred.t)
      -> handle_error:(Error.t -> unit)
      -> [ `Flushed of unit Deferred.t | `Connection_closed ]

    (** [schedule_dispatch_with_metadata] is like [schedule_dispatch] except that instead
        of the rpc metadata coming from metadata hooks added to the connection, it is
        passed directly. This is mostly useful if you are calling this function from an
        [`Expert] [on_unknown_rpc] handler (see {!Implementations.Expert.create}). *)
    val schedule_dispatch_with_metadata
      :  Connection.t
      -> rpc_tag:string
      -> version:int
      -> metadata:Rpc_metadata.V1.t option
      -> Bigstring.t
      -> pos:int
      -> len:int
      -> handle_response:(Bigstring.t -> pos:int -> len:int -> unit Deferred.t)
      -> handle_error:(Error.t -> unit)
      -> [ `Flushed of unit Deferred.t | `Connection_closed ]

    (** [dispatch] will raise if the length of the RPC message (i.e. [len] plus the
        message header) is longer than the underlying transport's max message size. *)
    val dispatch
      :  Connection.t
      -> rpc_tag:string
      -> version:int
      -> Bigstring.t
      -> pos:int
      -> len:int
      -> handle_response:(Bigstring.t -> pos:int -> len:int -> unit Deferred.t)
      -> handle_error:(Error.t -> unit)
      -> [ `Ok | `Connection_closed ]

    (** Result of callbacks passed to [implement] and [implement'] and
        [implement_for_tag_and_version] and [implement_for_tag_and_version']:

        - [Replied] means that the response has already been sent using one of the
          functions of [Responder]
        - [Delayed_response d] means that the implementation is done using the input
          bigstring, but hasn't send the response yet. When [d] becomes determined it is
          expected that the response has been sent.

        Note: it is NOT OK for an implementation to return:

        {[
          Delayed_response (Responder.schedule responder buf ~pos:... ~len:...)
        ]}

        where [buf] is the same bigstring as the one containing the query. This is because
        it would indicate that [buf] can be overwritten even though it is still being used
        by [Responder.schedule]. *)
    type implementation_result =
      | Replied
      | Delayed_response of unit Deferred.t

    val implement
      :  ?on_exception:On_exception.t
      -> (_, _) t
      -> ('connection_state
          -> Responder.t
          -> Bigstring.t
          -> pos:int
          -> len:int
          -> implementation_result Deferred.t)
      -> 'connection_state Implementation.t

    val implement'
      :  ?on_exception:On_exception.t
      -> (_, _) t
      -> ('connection_state
          -> Responder.t
          -> Bigstring.t
          -> pos:int
          -> len:int
          -> implementation_result)
      -> 'connection_state Implementation.t

    val implement_for_tag_and_version
      :  ?on_exception:On_exception.t
      -> rpc_tag:string
      -> version:int
      -> ('connection_state
          -> Responder.t
          -> Bigstring.t
          -> pos:int
          -> len:int
          -> implementation_result Deferred.t)
      -> 'connection_state Implementation.t

    val implement_for_tag_and_version'
      :  ?on_exception:On_exception.t
      -> rpc_tag:string
      -> version:int
      -> ('connection_state
          -> Responder.t
          -> Bigstring.t
          -> pos:int
          -> len:int
          -> implementation_result)
      -> 'connection_state Implementation.t
  end
end

module Pipe_close_reason : sig
  type t =
    | Closed_locally (** You closed the pipe. *)
    | Closed_remotely (** The RPC implementer closed the pipe. *)
    | Error of Error.t
    (** An error occurred, e.g. a message could not be deserialized. If the connection
        closes before either side explicitly closes the pipe, it will also go into this
        case. *)
  [@@deriving bin_io, compare, sexp]

  module Stable : sig
    module V1 : sig
      type nonrec t = t =
        | Closed_locally
        | Closed_remotely
        | Error of Error.Stable.V2.t
      [@@deriving bin_io, compare, sexp]
    end
  end
end

(** The input type of the [f] passed to [dispatch_iter]. *)
module Pipe_message : sig
  type 'a t =
    | Update of 'a
    | Closed of [ `By_remote_side | `Error of Error.t ]
end

(** The output type of the [f] passed to [dispatch_iter]. This is analogous to a simple
    [unit Deferred.t], with [Continue] being like [Deferred.unit], but it is made explicit
    when no waiting should occur.

    When [Wait ready] is returned, no data will be read from the [Connection.t] until
    [ready] becomes determined *)
module Pipe_response : sig
  type t =
    | Continue
    | Wait of unit Deferred.t
end

(** The result of dispatching a [Pipe_rpc] is a stream of responses instead of a single
    response. This can be used to implement long-running communication from a server to a
    client, or to transmit large datasets in incremental chunks.

    Once a [Pipe_rpc] implementation completes successfully, the server can stream
    response messages to the client until either one of the ends of the connection closes
    the stream, or the underlying [Rpc.Connection.t] closes. Pushback on the stream of
    responses can only expressed with connection-level pushback, e.g. TCP pushback when
    connecting over TCP.

    On both the dispatching and implementing ends, there are two models for using
    [Pipe_rpc]s: a [Pipe]-based model, and a direct model. The dispatch and implement ends
    for a given RPC don't need to use the same model, the underlying messages are the same
    either way and so the various approaches are inter-compatible.

    In general, the direct model has a less convenient interface, but is more performant,
    involves fewer allocations, and doesn't have to read/from write/to [Pipe.t]'s, which
    avoids the write barrier. If any of those properties would be useful, consider using
    the direct model instead of the pipe model.

    {3 Pipe model}
    In the pipe model, the implementer provides a ['response Pipe.Reader.t], and the
    client receives a ['response Pipe.Reader.t].

    {4 Implementing}
    [Async_rpc] will automatically iterate over batches of messages in the pipe and send
    them. It pulls up to 1_000 messages, writes them all to the underlying connection, and
    thens wait until those messages have been flushed before polling for more messages
    from the pipe.

    If the underlying connection gets backed up, e.g. because not enough bandwidth is
    available, the client is under load and can't read all the incoming messages, or
    intentionally pushes back on the stream and refuses to read more messages, then
    [Async_rpc] will stop pulling from the pipe. At that point, methods to fill the pipe
    that do respect pushback, like [Pipe.write] or [Pipe.map], will naturally block.
    Methods to fill the pipe that don't respect pushback, like
    [Pipe.write_without_pushback], will cause messages to buffer up inside the [Pipe], and
    can cause memory usage to grow in the server.

    {4 Dispatching}
    [Async_rpc] will create a pipe to return from [dispatch], and write all incoming
    messages to it. By default, [Async_rpc] won't pay attention to pushback on the pipe,
    it will just keep reading incoming messages from the connection and adding them to the
    pipe. This means that messages could end up being buffered locally, if the client
    isn't reading from the pipe fast enough.

    If [client_pushes_back] is set on the [Pipe_rpc], then if the client isn't reading
    from the pipe fast enough, [Async_rpc] will stop reading from the underlying
    connection until the contents of the pipe have been filled. See the docs on
    [Pipe_rpc.create] for more details.

    {3 Direct model}
    In the direct model, messages are written/read directly to/from the underlying
    connection.

    {4 Implementing}
    When implementing with [implement_direct], instead of returning a
    ['response Pipe.Reader.t], your implementation is given a
    ['response Direct_stream_writer.t], to which you can write messages with functions
    like [Direct_stream_writer.write]. Values written with a [Direct_stream_writer] are
    immediately serialized to the underlying connection. Pushback has to be handled
    manually, otherwise messages will be buffered in the transport's buffer waiting to be
    sent.

    {4 Dispatching}
    Using [dispatch_iter], you can provide a callback which will be called on each
    incoming message as soon as it's processed. This can be useful for avoiding async
    overhead in the case of many small messages, and can also be useful in cases where you
    care about the relative ordering of messages for different RPCs within the same
    connection, as the next message on the connection won't be processed until the
    callback is invoked.

    [dispatch_iter] also gives finegrained control over pushback on the connection. Note
    that pushback in this case pushes back on the entire connection, not just the one
    pipe. *)
module Pipe_rpc : sig
  type ('query, 'response, 'error) t

  module Id : sig
    type t
  end

  module Metadata : sig
    type t

    val id : t -> Id.t
  end

  (** @param client_pushes_back
        If the connection is backed up, the rpc server library stops consuming elements
        from the pipe being filled by [implement]'s caller. Servers should pay attention
        to the pipe's pushback, otherwise they risk running out of memory if they fill the
        pipe much faster than the transport can handle, or if the client pushes back as
        discussed next.

        If [client_pushes_back] is set, the client side of the connection will stop
        reading from the underlying file descriptor when the client's pipe has more
        elements enqueued than its [size_budget], waiting until [Pipe.downstream_flushed].
        This will eventually cause writes on the server's side to block, indicating to the
        server it should slow down.

        There are some drawbacks to using [client_pushes_back]:

        - RPC multiplexing doesn't work as well. The client will stop reading *all*
          messages on the connection if any pipe gets saturated, not just ones relating to
          that pipe.

        - This includes RPC heartbeats. If the pipe consumer takes too long to process
          items this will cause the RPC connection to closed with a "No heartbeats
          received" error.

        - A server that doesn't pay attention to pushback on its end will accumulate
          elements on its side of the connection, rather than on the client's side,
          meaning a slow client can make the server run out of memory.

        - NOTE that the client only resumes once [Pipe.downstream_flushed] completes,
          which is a stronger condition than just waiting on [Pipe.pushback]. This can be
          especially unintuitive if [Pipe.fork] is used on the response pipe, as
          [downstream_flushed] on [Pipe.fork] only becomes determined once both of the
          downstream pipes flush the elements. *)
  val create
    :  ?client_pushes_back:unit
    -> name:string
    -> version:int
    -> bin_query:'query Bin_prot.Type_class.t
    -> bin_response:'response Bin_prot.Type_class.t
    -> bin_error:'error Bin_prot.Type_class.t
    -> unit
    -> ('query, 'response, 'error) t

  val bin_query : ('query, _, _) t -> 'query Bin_prot.Type_class.t
  val bin_response : (_, 'response, _) t -> 'response Bin_prot.Type_class.t
  val bin_error : (_, _, 'error) t -> 'error Bin_prot.Type_class.t
  val shapes : (_, _, _) t -> Rpc_shapes.t

  (** The pipe returned by the implementation function will be closed automatically when
      either the connection to the client is closed or the client closes their pipe.

      As described in [create], elements in the returned pipe will be transferred to the
      underlying connection, waiting after each batch of elements in the pipe until those
      bytes have been flushed to the connection. Slow connections or [client_pushes_back]
      can cause elements to buffer in the pipe if the server doesn't respect pushback on
      the pipe. *)
  val implement
    :  ?here:Stdlib.Lexing.position
    -> ?on_exception:On_exception.t
    -> ?leave_open_on_exception:bool (* Default [false] *)
    -> ('query, 'response, 'error) t
    -> ('connection_state
        -> 'query
        -> ('response Pipe.Reader.t, 'error) Result.t Deferred.t)
    -> 'connection_state Implementation.t

  val implement_with_auth
    :  ?here:Stdlib.Lexing.position
    -> ?on_exception:On_exception.t
    -> ?leave_open_on_exception:bool (* Default [false] *)
    -> ('query, 'response, 'error) t
    -> ('connection_state
        -> 'query
        -> ('response Pipe.Reader.t, 'error) Result.t Or_not_authorized.t Deferred.t)
    -> 'connection_state Implementation.t

  (** A [Direct_stream_writer.t] is a simple object for responding to a [Pipe_rpc] or
      {!State_rpc} query, for use with functions below like [implement_direct].

      It is the most basic and primitive way to write data to a client recieivng the
      values.

      When compared to using a Pipe - there is effectively a [Pipe.iter] in the library
      that is writing those values to a direct stream writer.

      If your code makes it more natural to create a [Pipe.Reader.t] and provide that,
      then that's a reasonable thing to do, but generally if you don't already have a
      pipe, avoiding the hop going through the pipe by using a [Direct_stream_writer] will
      be more efficient, since the elements can be serialized immediately *)
  module Direct_stream_writer : sig
    type 'a t

    (** [write t x] returns [`Closed] if [t] is closed, or [`Flushed d] if it is open. In
        the open case, [d] is determined when the underlying [Transport.Writer.t] has been
        flushed. Note that if [t] is not started yet (i.e. [started t] is not determined),
        then [d] does not actually correspond to the message being flushed.

        Waiting on [`Flushed d] after every message is most likely a mistake, as that
        means you're only able to get one message through the stream for each flush of the
        underlying connection (e.g. per [write] syscall for a TCP socket-backed
        connection).

        Until [started t] is determined, calls to [write] and [write_without_pushback]
        will enqueue the messages until the RPC implementation completes. This can cause
        high memory usage and/or long async cycles when the messages do actually get
        serialized. *)
    val write : 'a t -> 'a -> [ `Flushed of unit Deferred.t | `Closed ]

    val write_without_pushback : 'a t -> 'a -> [ `Ok | `Closed ]

    (** [started t] will become determined once the implementation has completed
        successfully (i.e. the implementation returned a value and [Async_rpc] processed
        it and sent it to the client) and [t] has started to write stream updates,
        including having written all updates that were queued by calling one of the write
        functions before it was started.

        It is guaranteed that:
        - if [started t] is determined, any update written to [t] will immediately be
          serialized to the output stream.
        - if [started t] is not determined, then any update written to [t] will be
          enqueued until it is started.

        Note that if the implementation does not complete successfully, [started] may
        never become determined. *)
    val started : _ t -> unit Deferred.t

    val close : _ t -> unit
    val closed : _ t -> unit Deferred.t
    val flushed : _ t -> unit Deferred.t
    val is_closed : _ t -> bool

    module Expert : sig
      val write
        :  'a t
        -> buf:Bigstring.t
        -> pos:int
        -> len:int
        -> [ `Flushed of unit Deferred.t | `Closed ]

      val write_without_pushback
        :  'a t
        -> buf:Bigstring.t
        -> pos:int
        -> len:int
        -> [ `Ok | `Closed ]

      (** Similar to [write_without_pushback] but you may not modify the written portion
          of the bigstring until either:
          - [schedule_write] returns [`Closed] (indicating the connection is closed or the
            client aborted the rpc)
          - The returned deferred in the [`Flushed] case becomes determined *)
      val schedule_write
        :  'a t
        -> buf:Bigstring.t
        -> pos:int
        -> len:int
        -> [ `Flushed of unit Deferred.t Modes.Global.t | `Closed ]
    end

    (** Group of direct writers. Groups are optimized for sending the same message to
        multiple clients at once. *)
    module Group : sig
        type 'a direct_stream_writer
        type 'a t

        (** A group internally holds a buffer to serialize messages only once. This buffer
            will grow automatically to accomodate bigger messages.

            If [send_last_value_on_add:true], it is _not_ safe to share the same buffer
            between multiple groups, as the last value is kept in the buffer. *)
        module Buffer : sig
          type t

          val create : ?initial_size:int (* default 4096 *) -> unit -> t
        end

        val create : ?buffer:Buffer.t -> unit -> _ t

        (** [create_sending_last_value_on_add] will create a group that will automatically
            send a copy of the last value written to each new writer when it's added to
            the group. It's split out as a separate function from [create] as it's not
            safe to re-use a buffer between multiple different groups in this case, as the
            previous value is stored in the buffer. *)
        val create_sending_last_value_on_add : ?initial_buffer_size:int -> unit -> _ t

        (** [flushed_or_closed t] is determined when the underlying writer for each member
            of [t] is flushed or closed. *)
        val flushed_or_closed : _ t -> unit Deferred.t

        val flushed : _ t -> unit Deferred.t
        [@@deprecated "[since 2019-11] renamed as [flushed_or_closed]"]

        (** Add a direct stream writer to the group. Raises if the writer is closed or
            already part of the group, or if its bin-prot writer is different than an
            existing group member's. When the writer is closed, it is automatically
            removed from the group. *)
        val add_exn : 'a t -> 'a direct_stream_writer -> unit

        (** Remove a writer from a group. Note that writers are automatically removed from
            all groups when they are closed, so you only need to call this if you want to
            remove a writer without closing it. *)
        val remove : 'a t -> 'a direct_stream_writer -> unit

        (** Write a message on all direct writers in the group. Contrary to
            [Direct_stream_writer.write], this cannot return [`Closed] as elements of the
            group are removed immediately when they are closed.

            [write t x] is the same as [write_without_pushback t x; flushed t].

            Note: if the group was created with [~send_last_value_on_add:true], all write
            functions will save the value when written. If there are writers in the group
            at the time of writing, it will be serialized and saved to the buffer, but if
            there are no writers it will hold onto the ['a]. This means that it is unsafe
            to use if the ['a] is mutable or if it has a finalizer that is expected to be
            run *)
        val write : 'a t -> 'a -> unit Deferred.t

        val write_without_pushback : 'a t -> 'a -> unit
        val to_list : 'a t -> 'a direct_stream_writer list
        val length : _ t -> int

        (** When these functions are used with a group created with
            [~send_last_value_on_add:true], the group will save a copy of the relevant
            part of the buffer in order to send it to writers added in the future. *)
        module Expert : sig
          val write : 'a t -> buf:Bigstring.t -> pos:int -> len:int -> unit Deferred.t

          val write_without_pushback
            :  'a t
            -> buf:Bigstring.t
            -> pos:int
            -> len:int
            -> unit
        end
      end
      with type 'a direct_stream_writer := 'a t
  end

  (** Similar to [implement], but you are given the writer instead of providing a writer
      and the writer is a [Direct_stream_writer.t] instead of a [Pipe.Writer.t].

      The main advantage of this interface is that it consumes far less memory per open
      query.

      Though the implementation function is given a writer immediately, the result of the
      client's call to [dispatch] will not be determined until after the implementation
      function returns. Elements written before the function returns will be queued up to
      be written after the function returns. *)
  val implement_direct
    :  ?here:Stdlib.Lexing.position
    -> ?on_exception:On_exception.t
    -> ?leave_open_on_exception:bool (* Default [false] *)
    -> ('query, 'response, 'error) t
    -> ('connection_state
        -> 'query
        -> 'response Direct_stream_writer.t
        -> (unit, 'error) Result.t Deferred.t)
    -> 'connection_state Implementation.t

  val implement_direct_with_auth
    :  ?here:Stdlib.Lexing.position
    -> ?on_exception:On_exception.t
    -> ?leave_open_on_exception:bool (* Default [false] *)
    -> ('query, 'response, 'error) t
    -> ('connection_state
        -> 'query
        -> 'response Direct_stream_writer.t
        -> (unit, 'error) Result.t Or_not_authorized.t Deferred.t)
    -> 'connection_state Implementation.t

  (** This has [(..., 'error) Result.t] as its return type to represent the possibility of
      the call itself being somehow erroneous (but understood - the outer [Or_error.t]
      encompasses failures of that nature). Note that this cannot be done simply by making
      ['response] a result type, since [('response Pipe.Reader.t, 'error) Result.t] is
      distinct from [('response, 'error) Result.t Pipe.Reader.t].

      Note that the pipe will be closed if either of:
      - The implementer closes the pipe
      - The [Connection.t] is closed, either intentionally or due to a network error or
        other failure

      This means that it's possible for the pipe returned from [dispatch] to close before
      all the data that the server wanted to send has been received. If it's important to
      ensure that you got all the values the server intended to send, you should call
      [close_reason] with the provided [Metadata.t] to check why the pipe was closed.

      Closing the pipe has the effect of calling [abort]. *)
  val dispatch
    :  ('query, 'response, 'error) t
    -> Connection.t
    -> 'query
    -> ('response Pipe.Reader.t * Metadata.t, 'error) Result.t Or_error.t Deferred.t

  (** Like {!dispatch} but gives an {!Rpc_error.t} instead of an {!Error.t}. *)
  val dispatch'
    :  ('query, 'response, 'error) t
    -> Connection.t
    -> 'query
    -> ('response Pipe.Reader.t * Metadata.t, 'error) Result.t Rpc_result.t Deferred.t

  val dispatch_exn
    :  ('query, 'response, 'error) t
    -> Connection.t
    -> 'query
    -> ('response Pipe.Reader.t * Metadata.t) Deferred.t

  (** [dispatch] but requires handling of the [Pipe_close_reason] when there is an
      [Error]. [Closed_locally] and [Closed_remotely] are not considered errors. *)
  val dispatch_with_close_reason
    :  ('query, 'response, 'error) t
    -> Connection.t
    -> 'query
    -> (('response, Error.t) Pipe_with_writer_error.t, 'error) Result.t Or_error.t
         Deferred.t

  val dispatch_with_close_reason'
    :  ('query, 'response, 'error) t
    -> Connection.t
    -> 'query
    -> (('response, Error.t) Pipe_with_writer_error.t, 'error) Result.t Rpc_result.t
         Deferred.t

  (** Helper to convert the output of a regular [dispatch] into a
      [dispatch_with_close_reason]. Intended to allow wrapper libraries of [Async_rpc] to
      implement their own [dispatch_with_close_reason]. *)
  val pipe_with_writer_error_of_pipe_and_metadata
    :  'response Pipe.Reader.t * Metadata.t
    -> ('response, Error.t) Pipe_with_writer_error.t

  module Pipe_message = Pipe_message
  module Pipe_response = Pipe_response

  (** Calling [dispatch_iter t conn query ~f] is similar to calling
      [dispatch t conn query] and then iterating over the result pipe with [f]. The main
      advantage it offers is that its memory usage is much lower, making it more suitable
      for situations where many queries are open at once. [f] will be called on each
      message in the pipe as they're read off the underlying connection.

      [f] may be fed any number of [Update _] messages, followed by a single [Closed _]
      message.

      [f] can cause the connection to stop reading messages off of its underlying
      [Reader.t] by returning [Wait d]. It is NOT guaranteed that returning [Wait] will
      prevent [f] from being called again; returning [Wait] will cause async_rpc to stop
      reading new data in from the underlying connection, but any messages that have
      already been read in will still be processed and passed to [f].

      This is the same as what happens when a client stops reading from the pipe returned
      by [dispatch] when the [Pipe_rpc.t] has [client_pushes_back] set.

      When successful, [dispatch_iter] returns an [Id.t] after the subscription is
      started. This may be fed to [abort] with the same [Pipe_rpc.t] and [Connection.t] as
      the call to [dispatch_iter] to cancel the subscription, which will close the pipe on
      the implementation side. Calling it with a different [Pipe_rpc.t] or [Connection.t]
      has undefined behavior. *)
  val dispatch_iter
    :  ('query, 'response, 'error) t
    -> Connection.t
    -> 'query
    -> f:('response Pipe_message.t -> Pipe_response.t)
    -> (Id.t, 'error) Result.t Or_error.t Deferred.t

  module Expert : sig
    (** [Expert.dispatch_iter] is like [dispatch_iter] except it provides the buffer that
        the update is in directly. In some RPC transports, the buffer provided is the
        buffer that is being used to read from the connection, so it will eventually be
        re-used and the contents will be overwritten. It's guaranteed not to be re-used
        before either:
        - [f] completes, returning [Continue]
        - [f] completes, returning [Wait d], and [d] becomes determined

        [closed] will be called once, after which [f] will not be called.

        Like the non-expert [dispatch_iter], nothing will be read from the [Connection.t]
        between [f] returning [wait d] and [d] becoming determined.

        Note that unlike the non-expert [dispatch_iter], exceptions from [f] are caught,
        resulting in [closed] being invoked, instead of terminating the connection. *)
    val dispatch_iter
      :  ('query, 'response, 'error) t
      -> Connection.t
      -> 'query
      -> f:(Bigstring.t -> pos:int -> len:int -> Pipe_response.t)
      -> closed:([ `By_remote_side | `Error of Error.t ] -> unit)
      -> (Id.t, 'error) Result.t Or_error.t Deferred.t
  end

  (** [abort rpc connection id] given an RPC and the id returned as part of a call to
      dispatch, abort requests that the other side of the connection stop sending updates.

      If you are using [dispatch] rather than [dispatch_iter], you are encouraged to close
      the pipe you receive rather than calling [abort] -- both of these have the same
      effect. *)
  val abort : (_, _, _) t -> Connection.t -> Id.t -> unit

  (** [close_reason metadata] will be determined sometime after the pipe associated with
      [metadata] is closed. Its value will indicate what caused the pipe to be closed. *)
  val close_reason : Metadata.t -> Pipe_close_reason.t Deferred.t

  val client_pushes_back : (_, _, _) t -> bool
  val name : (_, _, _) t -> string
  val version : (_, _, _) t -> int
  val description : (_, _, _) t -> Description.t
  val query_type_id : ('query, _, _) t -> 'query Type_equal.Id.t
  val response_type_id : (_, 'response, _) t -> 'response Type_equal.Id.t
  val error_type_id : (_, _, 'error) t -> 'error Type_equal.Id.t
end

(** A state rpc is an easy way for two processes to synchronize a data structure by
    sending updates over the wire. It's basically a pipe rpc that sends/receives an
    initial state of the data structure, and then updates, and applies the updates under
    the covers. All the docs that apply to [Pipe_rpc] apply to [State_rpc] as well. *)
module State_rpc : sig
  type ('query, 'state, 'update, 'error) t

  module Id : sig
    type t
  end

  module Metadata : sig
    type t = Pipe_rpc.Metadata.t

    val id : t -> Id.t
  end

  val create
    :  ?client_pushes_back:unit
    -> name:string
    -> version:int
    -> bin_query:'query Bin_prot.Type_class.t
    -> bin_state:'state Bin_prot.Type_class.t
    -> bin_update:'update Bin_prot.Type_class.t
    -> bin_error:'error Bin_prot.Type_class.t
    -> unit
    -> ('query, 'state, 'update, 'error) t

  val bin_query : ('query, _, _, _) t -> 'query Bin_prot.Type_class.t
  val bin_state : (_, 'state, _, _) t -> 'state Bin_prot.Type_class.t
  val bin_update : (_, _, 'update, _) t -> 'update Bin_prot.Type_class.t
  val bin_error : (_, _, _, 'error) t -> 'error Bin_prot.Type_class.t
  val shapes : (_, _, _, _) t -> Rpc_shapes.t

  val implement
    :  ?here:Stdlib.Lexing.position
    -> ?on_exception:On_exception.t
    -> ?leave_open_on_exception:bool (* Default [false] *)
    -> ('query, 'state, 'update, 'error) t
    -> ('connection_state
        -> 'query
        -> ('state * 'update Pipe.Reader.t, 'error) Result.t Deferred.t)
    -> 'connection_state Implementation.t

  val implement_with_auth
    :  ?here:Stdlib.Lexing.position
    -> ?on_exception:On_exception.t
    -> ?leave_open_on_exception:bool (* Default [false] *)
    -> ('query, 'state, 'update, 'error) t
    -> ('connection_state
        -> 'query
        -> ('state * 'update Pipe.Reader.t, 'error) Result.t Or_not_authorized.t
             Deferred.t)
    -> 'connection_state Implementation.t

  val implement_direct
    :  ?here:Stdlib.Lexing.position
    -> ?on_exception:On_exception.t
    -> ?leave_open_on_exception:bool (* Default [false] *)
    -> ('query, 'state, 'update, 'error) t
    -> ('connection_state
        -> 'query
        -> 'update Pipe_rpc.Direct_stream_writer.t
        -> ('state, 'error) Result.t Deferred.t)
    -> 'connection_state Implementation.t

  val implement_direct_with_auth
    :  ?here:Stdlib.Lexing.position
    -> ?on_exception:On_exception.t
    -> ?leave_open_on_exception:bool (* Default [false] *)
    -> ('query, 'state, 'update, 'error) t
    -> ('connection_state
        -> 'query
        -> 'update Pipe_rpc.Direct_stream_writer.t
        -> ('state, 'error) Result.t Or_not_authorized.t Deferred.t)
    -> 'connection_state Implementation.t

  val dispatch
    :  ('query, 'state, 'update, 'error) t
    -> Connection.t
    -> 'query
    -> ('state * 'update Pipe.Reader.t * Metadata.t, 'error) Result.t Or_error.t
         Deferred.t

  (** [dispatch] but requires handling of the [Pipe_close_reason] when there is an
      [Error]. [Closed_locally] and [Closed_remotely] are not considered errors. *)
  val dispatch_with_close_reason
    :  ('query, 'state, 'update, 'error) t
    -> Connection.t
    -> 'query
    -> ('state * ('update, Error.t) Pipe_with_writer_error.t, 'error) Result.t Or_error.t
         Deferred.t

  module Pipe_message = Pipe_message
  module Pipe_response = Pipe_response

  (** [dispatch_fold] is similar to [Pipe_rpc.dispatch_iter]. [init] will be called with
      the initial state, and then [f] will be called on each update message. If the update
      pipe is closed either by the implementer or an error, [closed] will be invoked on
      the last accumulated value to produce the result. The update stream can be stopped
      with [abort], in which case [closed] will not be called and the ['result Deferred.t]
      will never become determined. *)
  val dispatch_fold
    :  ('query, 'state, 'update, 'error) t
    -> Connection.t
    -> 'query
    -> init:('state -> 'acc)
    -> f:('acc -> 'update -> 'acc * Pipe_response.t)
    -> closed:('acc -> [ `By_remote_side | `Error of Error.t ] -> 'result)
    -> (Id.t * 'result Deferred.t, 'error) Result.t Or_error.t Deferred.t

  val dispatch'
    :  ('query, 'state, 'update, 'error) t
    -> Connection.t
    -> 'query
    -> ('state * 'update Pipe.Reader.t * Metadata.t, 'error) Result.t Rpc_result.t
         Deferred.t

  (** [dispatch] but requires handling of the [Pipe_close_reason] when there is an
      [Error]. [Closed_locally] and [Closed_remotely] are not considered errors. *)
  val dispatch_with_close_reason'
    :  ('query, 'state, 'update, 'error) t
    -> Connection.t
    -> 'query
    -> ('state * ('update, Error.t) Pipe_with_writer_error.t, 'error) Result.t
         Rpc_result.t
         Deferred.t

  val abort : (_, _, _, _) t -> Connection.t -> Id.t -> unit
  val close_reason : Metadata.t -> Pipe_close_reason.t Deferred.t
  val client_pushes_back : (_, _, _, _) t -> bool
  val name : (_, _, _, _) t -> string
  val version : (_, _, _, _) t -> int
  val description : (_, _, _, _) t -> Description.t
  val query_type_id : ('query, _, _, _) t -> 'query Type_equal.Id.t
  val state_type_id : (_, 'state, _, _) t -> 'state Type_equal.Id.t
  val update_type_id : (_, _, 'update, _) t -> 'update Type_equal.Id.t
  val error_type_id : (_, _, _, 'error) t -> 'error Type_equal.Id.t
end

(** An RPC that has no response. Error handling is trickier here than it is for RPCs with
    responses, as there is no reasonable place to put an error if something goes wrong.
    Because of this, in the event of an error such as dispatching to an unimplemented RPC,
    the connection will be shut down. *)
module One_way : sig
  type 'msg t

  val create : name:string -> version:int -> bin_msg:'msg Bin_prot.Type_class.t -> 'msg t
  val name : _ t -> string
  val version : _ t -> int
  val description : _ t -> Description.t
  val msg_type_id : 'msg t -> 'msg Type_equal.Id.t
  val bin_msg : 'msg t -> 'msg Bin_prot.Type_class.t
  val shapes : _ t -> Rpc_shapes.t

  val implement
    :  ?here:Stdlib.Lexing.position
    -> ?on_exception:On_exception.t
    -> 'msg t
    -> ('connection_state -> 'msg -> unit)
    -> 'connection_state Implementation.t

  (** [dispatch'] exposes [Rpc_result.t] as output. Passing it through
      [rpc_result_to_or_error] gives you the same result as [dispatch] *)
  val dispatch' : 'msg t -> Connection.t -> 'msg -> unit Rpc_result.t

  val rpc_result_to_or_error
    :  'msg t
    -> Connection.t
    -> unit Rpc_result.t
    -> unit Or_error.t

  val dispatch : 'msg t -> Connection.t -> 'msg -> unit Or_error.t
  val dispatch_exn : 'msg t -> Connection.t -> 'msg -> unit

  module Expert : sig
    val implement
      :  ?on_exception:On_exception.t
      -> _ t
      -> ('connection_state -> Bigstring.t -> pos:int -> len:int -> unit)
      -> 'connection_state Implementation.t

    val dispatch
      :  _ t
      -> Connection.t
      -> Bigstring.t
      -> pos:int
      -> len:int
      -> [ `Ok | `Connection_closed ]

    (** Like [dispatch], but does not copy data out of the buffer, so it must not change
        until the returned [unit Deferred.t] is determined. *)
    val schedule_dispatch
      :  _ t
      -> Connection.t
      -> Bigstring.t
      -> pos:int
      -> len:int
      -> [ `Flushed of unit Deferred.t | `Connection_closed ]
  end
end

module Any : sig
  type t =
    | Rpc : ('q, 'r) Rpc.t -> t
    | Pipe : ('q, 'r, 'e) Pipe_rpc.t -> t
    | State : ('q, 's, 'u, 'e) State_rpc.t -> t
    | One_way : 'm One_way.t -> t

  val description : t -> Description.t
end

module Stable : sig
  module Rpc : sig
    type ('query, 'response) t = ('query, 'response) Rpc.t

    val create
      :  name:string
      -> version:int
      -> bin_query:'query Bin_prot.Type_class.t
      -> bin_response:'response Bin_prot.Type_class.t
      -> include_in_error_count:'response How_to_recognise_errors.t
      -> ('query, 'response) t

    val description : (_, _) t -> Description.t
    val bin_query : ('query, _) t -> 'query Bin_prot.Type_class.t
    val bin_response : (_, 'response) t -> 'response Bin_prot.Type_class.t
  end

  module Pipe_rpc : sig
    type ('query, 'response, 'error) t = ('query, 'response, 'error) Pipe_rpc.t

    val create
      :  ?client_pushes_back:unit
      -> name:string
      -> version:int
      -> bin_query:'query Bin_prot.Type_class.t
      -> bin_response:'response Bin_prot.Type_class.t
      -> bin_error:'error Bin_prot.Type_class.t
      -> unit
      -> ('query, 'response, 'error) t

    val description : (_, _, _) t -> Description.t
    val bin_query : ('query, _, _) t -> 'query Bin_prot.Type_class.t
    val bin_response : (_, 'response, _) t -> 'response Bin_prot.Type_class.t
    val bin_error : (_, _, 'error) t -> 'error Bin_prot.Type_class.t
  end

  module State_rpc : sig
    type ('query, 'state, 'update, 'error) t =
      ('query, 'state, 'update, 'error) State_rpc.t

    val create
      :  ?client_pushes_back:unit
      -> name:string
      -> version:int
      -> bin_query:'query Bin_prot.Type_class.t
      -> bin_state:'state Bin_prot.Type_class.t
      -> bin_update:'update Bin_prot.Type_class.t
      -> bin_error:'error Bin_prot.Type_class.t
      -> unit
      -> ('query, 'state, 'update, 'error) t

    val description : (_, _, _, _) t -> Description.t
    val bin_query : ('query, _, _, _) t -> 'query Bin_prot.Type_class.t
    val bin_state : (_, 'state, _, _) t -> 'state Bin_prot.Type_class.t
    val bin_update : (_, _, 'update, _) t -> 'update Bin_prot.Type_class.t
    val bin_error : (_, _, _, 'error) t -> 'error Bin_prot.Type_class.t
  end

  module One_way : sig
    type 'msg t = 'msg One_way.t

    val create
      :  name:string
      -> version:int
      -> bin_msg:'msg Bin_prot.Type_class.t
      -> 'msg t

    val description : _ t -> Description.t
    val bin_msg : 'msg t -> 'msg Bin_prot.Type_class.t
  end

  module Description = Description.Stable
  module Pipe_close_reason = Pipe_close_reason.Stable
end
