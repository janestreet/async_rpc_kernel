(** Internal to [Async_rpc_kernel]. See [Rpc.Implementations]. *)

open! Core
open! Async_kernel
open Protocol

type 'a t

type 'connection_state on_unknown_rpc =
  [ `Raise
  | `Continue
  | `Close_connection
  | `Call of
    'connection_state
    -> rpc_tag:string
    -> version:int
    -> [ `Close_connection | `Continue ]
  ]

val create
  :  implementations:'connection_state Implementation.t list
  -> on_unknown_rpc:'connection_state on_unknown_rpc
  -> on_exception:On_exception.t
  -> ('connection_state t, [ `Duplicate_implementations of Description.t list ]) Result.t

val null : unit -> 'a t
val lift : 'a t -> f:('b -> 'a) -> 'b t

module Direct_stream_writer : sig
  type 'a t = 'a Implementation_types.Direct_stream_writer.t

  module Id = Implementation_types.Direct_stream_writer.Id

  val started : _ t -> unit Deferred.t
  val close : ?result:[ `Eof ] Rpc_result.t (* Default [Ok `Eof] *) -> _ t -> unit
  val closed : _ t -> unit Deferred.t
  val is_closed : _ t -> bool
  val write : 'a t -> 'a -> [ `Flushed of unit Deferred.t | `Closed ]
  val write_without_pushback : 'a t -> 'a -> [ `Ok | `Closed ]
  val flushed : _ t -> unit Deferred.t
  val bin_writer : 'a t -> 'a Bin_prot.Type_class.writer

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

    val schedule_write
      :  'a t
      -> buf:Bigstring.t
      -> pos:int
      -> len:int
      -> local_ [ `Flushed of unit Deferred.t Modes.Global.t | `Closed ]
  end
end

module Instance : sig
  type t [@@deriving sexp_of]

  val handle_query
    :  t
    -> query:Nat0.t Query.Validated.t
    -> read_buffer:Bigstring.t
    -> read_buffer_pos_ref:int ref
    -> close_connection_monitor:Monitor.t
    -> message_bytes_for_tracing:int
    -> unit Rpc_result.t Transport.Handler_result.t

  (* Flushes all open streaming responses *)
  val flush : t -> unit Deferred.t

  (** Return the number of outstanding queries. *)
  val open_queries : t -> int

  (* Stop the instance: drop all responses to pending requests and make all further call
     to [handle_query] or [flush] to fail. *)
  val stop : t -> unit
  val get_description_from_menu_rank : t -> int -> local_ Description.t global option
end

val instantiate
  :  'a t
  -> menu:Menu.t option
  -> connection_description:Info.t
  -> connection_close_started:Close_reason.t Deferred.t
  -> connection_state:'a
  -> writer:Protocol_writer.t
  -> tracing_events:(local_ Tracing_event.t -> unit) Bus.Read_write.t
  -> on_receive:
       (local_ Description.t
        -> query_id:Query_id.t
        -> Rpc_metadata.V2.t option
        -> Execution_context.t
        -> Execution_context.t)
  -> no_open_queries_event:(unit, read_write) Bvar.t
  -> Instance.t

val create_exn
  :  implementations:'connection_state Implementation.t list
  -> on_unknown_rpc:
       [ `Raise
       | `Continue
       | `Close_connection
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

val remove_exn
  :  'connection_state t
  -> Description.t
  -> 'connection_state Implementation.t * 'connection_state t

val find
  :  'connection_state t
  -> Description.t
  -> 'connection_state Implementation.t option

val descriptions : _ t -> Description.t list

val descriptions_and_shapes
  :  ?exclude_name:string
  -> _ t
  -> (Description.t * Rpc_shapes.Just_digests.t) list

val map_implementations
  :  'a t
  -> f:('a Implementation.t list -> 'a Implementation.t list)
  -> ('a t, [ `Duplicate_implementations of Description.t list ]) Result.t

module Expert : sig
  module Responder = Implementation.Expert.Responder

  module Rpc_responder : sig
    type t = Responder.t

    val schedule
      :  t
      -> Bigstring.t
      -> pos:int
      -> len:int
      -> [ `Connection_closed | `Flushed of unit Deferred.t ]

    val write_bigstring : t -> Bigstring.t -> pos:int -> len:int -> unit
    val write_bin_prot : t -> 'a Bin_prot.Type_class.writer -> 'a -> unit
    val write_error : t -> Error.t -> unit
  end

  val create_exn
    :  implementations:'connection_state Implementation.t list
    -> on_unknown_rpc:
         [ `Raise
         | `Continue
         | `Close_connection
         | `Call of
           'connection_state
           -> rpc_tag:string
           -> version:int
           -> [ `Close_connection | `Continue ]
         | `Expert of
           'connection_state
           -> rpc_tag:string
           -> version:int
           -> metadata:Rpc_metadata.V2.t option
           -> Responder.t
           -> Bigstring.t
           -> pos:int
           -> len:int
           -> unit Deferred.t
         ]
    -> on_exception:On_exception.t
    -> 'connection_state t
end

module Private : sig
  val to_implementation_list
    :  'connection_state t
    -> 'connection_state Implementation.t list
       * [ `Raise
         | `Continue
         | `Close_connection
         | `Call of
           'connection_state
           -> rpc_tag:string
           -> version:int
           -> [ `Close_connection | `Continue ]
         | `Expert of
           'connection_state
           -> rpc_tag:string
           -> version:int
           -> metadata:Rpc_metadata.V2.t option
           -> Expert.Responder.t
           -> Bigstring.t
           -> pos:int
           -> len:int
           -> unit Deferred.t
         ]
       * On_exception.t
end
