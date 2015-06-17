(** A library for building asynchronous RPC-style protocols.

    The approach here is to have a separate representation of the server-side
    implementation of an RPC (An [Implementation.t]) and the interface that it exports
    (either an [Rpc.t], a [State_rpc.t] or a [Pipe_rpc.t], but we'll refer to them
    generically as RPC interfaces).  A server builds the [Implementation.t] out of an RPC
    interface and a function for implementing the RPC, while the client dispatches a
    request using the same RPC interface.

    The [Implementation.t] hides the type of the query and the response, whereas the
    [Rpc.t] is polymorphic in the query and response type.  This allows you to build a
    [Implementations.t] out of a list of [Implementation.t]s.

    Each RPC also comes with a version number.  This is meant to allow support of multiple
    different versions of what is essentially the same RPC.  You can think of it as an
    extension to the name of the RPC, and in fact, each RPC is uniquely identified by its
    (name, version) pair.  RPCs with the same name but different versions should implement
    similar functionality. *)

open Core_kernel.Std
open Async_kernel.Std

module Description : sig
  type t =
    { name    : string
    ; version : int
    }
  with compare, sexp_of

  module Stable : sig
    module V1 : sig
      type nonrec t = t with compare, sexp, bin_io
    end
  end
end

(** A ['connection_state Implementation.t] is something that knows how to respond to one
    query, given a ['connection_state].  That is, you can create a ['connection_state
    Implementation.t] by providing a function which takes a query *and* a
    ['connection_state] and provides a response.

    The reason for this is that RPCs often do something like look something up in a master
    structure.  This way, [Implementation.t]s can be created without having the master
    structure in your hands. *)
module Implementation : sig
  type 'connection_state t

  val description : _ t -> Description.t

  (** We may want to use an ['a t] implementation (perhaps provided by someone else) in a
      ['b t] context.  We can do this as long as we can map our state into the state
      expected by the original implementer. *)
  val lift : 'a t -> f:('b -> 'a) -> 'b t
end

(** A ['connection_state Implementations.t] is something that knows how to respond to
    many different queries.  It is conceptually a package of ['connection_state
    Implementation.t]s. *)
module Implementations : sig
  type 'connection_state t = 'connection_state Implementations.t

  (** a server that can handle no queries *)
  val null : unit -> 'connection_state t

  val lift : 'a t -> f:('b -> 'a) -> 'b t

  (** [create ~implementations ~on_unknown_rpc] creates a server capable of responding to
      the rpcs implemented in the implementation list.  Be careful about setting
      [on_unknown_rpc] to [`Raise] because other programs may mistakenly connect to this
      one causing it to crash. *)
  val create
    :  implementations : 'connection_state Implementation.t list
    -> on_unknown_rpc :
      [ `Raise
      | `Continue
      | `Close_connection  (** used to be the behavior of [`Ignore] *)
      (** [rpc_tag] and [version] are the name and version of the unknown rpc *)
      | `Call of
          ('connection_state
           -> rpc_tag : string
           -> version : int
           -> [ `Close_connection | `Continue ])
      ]
    -> ( 'connection_state t
       , [`Duplicate_implementations of Description.t list]
       ) Result.t

  val create_exn
    :  implementations : 'connection_state Implementation.t list
    -> on_unknown_rpc :
      [ `Raise
      | `Continue
      | `Close_connection  (** used to be the behavior of [`Ignore] *)
      | `Call of
          ('connection_state
           -> rpc_tag : string
           -> version : int
           -> [ `Close_connection | `Continue ])
      ]
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

  (** Low-level, untyped access to queries.  Regular users should ignore this. *)
  module Expert : sig
    (** See [Rpc.Expert.Responder] for how to use this. *)
    module Responder : sig
      type t
    end

    (** Same as [create_exn], except for the additional [`Expert] variant. *)
    val create_exn
      :  implementations : 'connection_state Implementation.t list
      -> on_unknown_rpc :
        [ `Raise
        | `Continue
        | `Close_connection  (** used to be the behavior of [`Ignore] *)
        | `Call of
            ('connection_state
             -> rpc_tag : string
             -> version : int
             -> [ `Close_connection | `Continue ])
        | `Expert of
            (** The [Deferred.t] the function returns is only used to determine when it is
                safe to overwrite the supplied [Bigstring.t], so it is *not* necessary to
                completely finish handling the query before it is filled in.  In
                particular, if you don't intend to read from the [Bigstring.t] after the
                function returns, you can return [Deferred.unit]. *)
            ('connection_state
             -> rpc_tag : string
             -> version : int
             -> Responder.t
             -> Bigstring.t
             -> pos : int
             -> len : int
             -> unit Deferred.t)
        ]
      -> 'connection_state t
  end
end

module Transport = Transport

module Connection : Connection_intf.S

module Rpc : sig
  type ('query, 'response) t

  val create
    :  name         : string
    -> version      : int
    -> bin_query    : 'query    Bin_prot.Type_class.t
    -> bin_response : 'response Bin_prot.Type_class.t
    -> ('query, 'response) t

  (** the same values as were passed to create. *)
  val name    : (_, _) t -> string
  val version : (_, _) t -> int

  val description : (_, _) t -> Description.t

  val bin_query    : ('query, _)    t -> 'query    Bin_prot.Type_class.t
  val bin_response : (_, 'response) t -> 'response Bin_prot.Type_class.t

  val implement
    :  ('query, 'response) t
    -> ('connection_state
        -> 'query
        -> 'response Deferred.t)
    -> 'connection_state Implementation.t

  (** [implement'] is different from [implement] in that:

      1. ['response] is immediately serialized and scheduled for delivery to the RPC
         dispatcher.

      2. Less allocation happens, as none of the Async-related machinery is necessary.

      [implement] also tries to do 1 when possible, but it is guaranteed to happen with
      [implement']. *)
  val implement'
    :  ('query, 'response) t
    -> ('connection_state
        -> 'query
        -> 'response)
    -> 'connection_state Implementation.t

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
        -> pos : int
        -> len : int
        -> [`Flushed of unit Deferred.t | `Connection_closed]

      (** On the other hand, these are written immediately. *)
      val write_bigstring : t -> Bigstring.t -> pos:int -> len:int -> unit
      val write_bin_prot  : t -> 'a Bin_prot.Type_class.writer -> 'a -> unit
      val write_error     : t -> Error.t -> unit
    end

    (** This just schedules a write, so the [Bigstring.t] should not be overwritten until
        the flushed [Deferred.t] is determined.

        The return value of [handle_response] has the same meaning as in the function
        argument of [Implementations.Expert.create]. *)
    val schedule_dispatch
      :  Connection.t
      -> rpc_tag : string
      -> version : int
      -> Bigstring.t
      -> pos : int
      -> len : int
      -> handle_response : (Bigstring.t -> pos:int -> len:int -> unit Deferred.t)
      -> handle_error : (Error.t -> unit)
      -> [`Flushed of unit Deferred.t | `Connection_closed]

    val dispatch
      :  Connection.t
      -> rpc_tag : string
      -> version : int
      -> Bigstring.t
      -> pos : int
      -> len : int
      -> handle_response : (Bigstring.t -> pos:int -> len:int -> unit Deferred.t)
      -> handle_error : (Error.t -> unit)
      -> unit

  end
end

module Pipe_rpc : sig
  type ('query, 'response, 'error) t

  module Id : sig type t end

  val create
    (** If [client_pushes_back] is set, the client side of the connection will stop
        reading elements from the underlying file descriptor when the client's pipe has
        a sufficient number of elements enqueued, rather than reading elements eagerly.
        This will eventually cause writes on the server's side to stop working, which
        gives the server an indication when a client is backed up.

        Setting this allows a careful server to notice when its clients are unable to keep
        up and slow down its work accordingly.  However, it has some drawbacks:

        - RPC multiplexing doesn't work as well.  The client will stop reading *all*
        messages on the connection if any pipe gets saturated, not just ones relating
        to that pipe.

        - A server that doesn't pay attention to pushback on its end will accumulate
        elements on its side of the connection, rather than on the client's side,
        meaning a slow client can make the server run out of memory. *)
    :  ?client_pushes_back : unit
    -> name : string
    -> version : int
    -> bin_query    : 'query    Bin_prot.Type_class.t
    -> bin_response : 'response Bin_prot.Type_class.t
    -> bin_error    : 'error    Bin_prot.Type_class.t
    -> unit
    -> ('query, 'response, 'error) t

  val bin_query    : ('query, _, _) t    -> 'query    Bin_prot.Type_class.t
  val bin_response : (_, 'response, _) t -> 'response Bin_prot.Type_class.t
  val bin_error    : (_, _, 'error) t    -> 'error    Bin_prot.Type_class.t

  val implement
    :  ('query, 'response, 'error) t
    -> ('connection_state
        -> 'query
        -> aborted : unit Deferred.t
        -> ('response Pipe.Reader.t, 'error) Result.t Deferred.t)
    -> 'connection_state Implementation.t

  (** This has [(..., 'error) Result.t] as its return type to represent the possibility of
      the call itself being somehow erroneous (but understood - the outer [Or_error.t]
      encompasses failures of that nature).  Note that this cannot be done simply by
      making ['response] a result type, since [('response Pipe.Reader.t, 'error) Result.t]
      is distinct from [('response, 'error) Result.t Pipe.Reader.t].

      Closing the pipe has the effect of calling [abort]. *)
  val dispatch
    :  ('query, 'response, 'error) t
    -> Connection.t
    -> 'query
    -> ('response Pipe.Reader.t * Id.t, 'error) Result.t Or_error.t Deferred.t

  val dispatch_exn
    :  ('query, 'response, 'error) t
    -> Connection.t
    -> 'query
    -> ('response Pipe.Reader.t * Id.t) Deferred.t

  (** [abort rpc connection id] given an RPC and the id returned as part of a call to
      dispatch, abort requests that the other side of the connection stop sending
      updates. *)
  val abort : (_, _, _) t -> Connection.t -> Id.t -> unit

  val name    : (_, _, _) t -> string
  val version : (_, _, _) t -> int

  val description : (_, _, _) t -> Description.t


end

(** A state rpc is an easy way for two processes to synchronize a data structure by
    sending updates over the wire.  It's basically a pipe rpc that sends/receives an
    initial state of the data structure, and then updates, and applies the updates under
    the covers. *)
module State_rpc : sig
  type ('query, 'state, 'update, 'error) t

  module Id : sig type t end

  val create
    :  ?client_pushes_back : unit
    -> name : string
    -> version : int
    -> bin_query  : 'query  Bin_prot.Type_class.t
    -> bin_state  : 'state  Bin_prot.Type_class.t
    -> bin_update : 'update Bin_prot.Type_class.t
    -> bin_error  : 'error  Bin_prot.Type_class.t
    -> unit
    -> ('query, 'state, 'update, 'error) t

  val bin_query  : ('query, _, _, _)  t -> 'query  Bin_prot.Type_class.t
  val bin_state  : (_, 'state, _, _)  t -> 'state  Bin_prot.Type_class.t
  val bin_update : (_, _, 'update, _) t -> 'update Bin_prot.Type_class.t
  val bin_error  : (_, _, _, 'error)  t -> 'error  Bin_prot.Type_class.t

  val implement
    :  ('query, 'state, 'update, 'error) t
    -> ('connection_state
        -> 'query
        -> aborted : unit Deferred.t
        -> (('state * 'update Pipe.Reader.t), 'error) Result.t Deferred.t)
    -> 'connection_state Implementation.t

  val dispatch
    :  ('query, 'state, 'update, 'error) t
    -> Connection.t
    -> 'query
    -> update : ('state -> 'update -> 'state)
    -> ( 'state * ('state * 'update) Pipe.Reader.t * Id.t
       , 'error
       ) Result.t Or_error.t Deferred.t

  val abort : (_, _, _, _) t -> Connection.t -> Id.t -> unit

  val name    : (_, _, _, _) t -> string
  val version : (_, _, _, _) t -> int

  val description : (_, _, _, _) t -> Description.t
end

(** An RPC that has no response.  Error handling is trickier here than it is for RPCs with
    responses, as there is no reasonable place to put an error if something goes wrong.
    Because of this, in the event of an error such as dispatching to an unimplemented RPC,
    the connection will be shut down.  Similarly, if the implementation raises an
    exception, the connection will be shut down. *)
module One_way : sig
  type 'msg t

  val create
    :  name     : string
    -> version  : int
    -> bin_msg  : 'msg Bin_prot.Type_class.t
    -> 'msg t

  val name        : _ t -> string
  val version     : _ t -> int
  val description : _ t -> Description.t
  val bin_msg     : 'msg t -> 'msg Bin_prot.Type_class.t

  val implement
    :  'msg t
    -> ('connection_state -> 'msg -> unit)
    -> 'connection_state Implementation.t

  val dispatch
    :  'msg t
    -> Connection.t
    -> 'msg
    -> unit Or_error.t

  val dispatch_exn
    :  'msg t
    -> Connection.t
    -> 'msg
    -> unit

  module Expert : sig
    val implement
      :  _ t
      -> ('connection_state
          -> Bigstring.t
          -> pos : int
          -> len : int
          -> unit)
      -> 'connection_state Implementation.t

    val dispatch
      :  _ t
      -> Connection.t
      -> Bigstring.t
      -> pos : int
      -> len : int
      -> [`Ok | `Connection_closed]

    (** Like [dispatch], but does not copy data out of the buffer, so it must not change
        until the returned [unit Deferred.t] is determined. *)
    val schedule_dispatch
      :  _ t
      -> Connection.t
      -> Bigstring.t
      -> pos : int
      -> len : int
      -> [`Flushed of unit Deferred.t | `Connection_closed]
  end
end

module Any : sig
  type t =
    | Rpc     : ('q, 'r) Rpc.t -> t
    | Pipe    : ('q, 'r, 'e) Pipe_rpc.t -> t
    | State   : ('q, 's, 'u, 'e) State_rpc.t -> t
    | One_way : 'm One_way.t -> t
end
