(** Internal to [Async_rpc_kernel].  See [Rpc.Implementation]. *)

open! Core
open! Async_kernel
open Protocol
open Implementation_types.Implementation

module Expert : sig
  module Responder : sig
    type t = Expert.Responder.t =
      { query_id : Query_id.t
      ; writer : Protocol_writer.t
      ; mutable responded : bool
      }
    [@@deriving sexp_of]

    val create : Query_id.t -> Protocol_writer.t -> t
  end

  type implementation_result = Expert.implementation_result =
    | Replied
    | Delayed_response of unit Deferred.t
end

module F : sig
  type 'a error_mode = 'a F.error_mode =
    | Always_ok : _ error_mode
    | Using_result : (_, _) Result.t error_mode
    | Using_result_result : ((_, _) Result.t, _) Result.t error_mode
    | Is_error : ('a -> bool) -> 'a error_mode
    | Streaming_initial_message : (_, _) Protocol.Stream_initial_message.t error_mode

  type ('a, 'b) result_mode = ('a, 'b) F.result_mode =
    | Blocking : ('a, 'a Or_not_authorized.t) result_mode
    | Deferred : ('a, 'a Or_not_authorized.t Deferred.t) result_mode

  type ('connection_state, 'query, 'init, 'update) streaming_impl =
        ('connection_state, 'query, 'init, 'update) F.streaming_impl =
    | Pipe of
        ('connection_state
         -> 'query
         -> ('init * 'update Pipe.Reader.t, 'init) Result.t Or_not_authorized.t Deferred.t)
    | Direct of
        ('connection_state
         -> 'query
         -> 'update Implementation_types.Direct_stream_writer.t
         -> ('init, 'init) Result.t Or_not_authorized.t Deferred.t)

  type ('connection_state, 'query, 'init, 'update) streaming_rpc =
        ('connection_state, 'query, 'init, 'update) F.streaming_rpc =
    { bin_query_reader : 'query Bin_prot.Type_class.reader
    ; bin_init_writer : 'init Bin_prot.Type_class.writer
    ; bin_update_writer : 'update Bin_prot.Type_class.writer
        (* 'init can be an error or an initial state *)
    ; impl : ('connection_state, 'query, 'init, 'update) streaming_impl
    ; error_mode : 'init error_mode
    }

  type 'connection_state t = 'connection_state F.t =
    | One_way :
        'msg Bin_prot.Type_class.reader
        * ('connection_state -> 'msg -> unit Or_not_authorized.t Deferred.t)
        -> 'connection_state t
    | One_way_expert :
        ('connection_state
         -> Bigstring.t
         -> pos:int
         -> len:int
         -> unit Or_not_authorized.t Deferred.t)
        -> 'connection_state t
    | Rpc :
        'query Bin_prot.Type_class.reader
        * 'response Bin_prot.Type_class.writer
        * ('connection_state -> 'query -> 'result)
        * 'response error_mode
        * ('response, 'result) result_mode
        -> 'connection_state t
    | Rpc_expert :
        ('connection_state
         -> Expert.Responder.t
         -> Bigstring.t
         -> pos:int
         -> len:int
         -> 'result)
        * (Expert.implementation_result, 'result) result_mode
        -> 'connection_state t
    | Streaming_rpc :
        ('connection_state, 'query, 'init, 'update) streaming_rpc
        -> 'connection_state t
    | Legacy_menu_rpc : Menu.Stable.V2.response Lazy.t -> 'connection_state t

  val lift : 'a t -> f:('b -> 'a Or_not_authorized.t) -> 'b t
  val lift_deferred : 'a t -> f:('b -> 'a Or_not_authorized.t Deferred.t) -> 'b t
end

type 'connection_state t = 'connection_state Implementation_types.Implementation.t =
  { tag : Rpc_tag.t
  ; version : int
  ; f : 'connection_state F.t
  ; shapes : (Rpc_shapes.t * Rpc_shapes.Just_digests.t) Lazy.t
  ; on_exception : On_exception.t
  }
[@@deriving sexp_of]

val description : _ t -> Description.t
val shapes : _ t -> Rpc_shapes.t
val digests : _ t -> Rpc_shapes.Just_digests.t
val lift : 'a t -> f:('b -> 'a) -> 'b t
val lift_deferred : 'a t -> f:('b -> 'a Deferred.t) -> 'b t
val with_authorization : 'a t -> f:('b -> 'a Or_not_authorized.t) -> 'b t

val with_authorization_deferred
  :  'a t
  -> f:('b -> 'a Or_not_authorized.t Deferred.t)
  -> 'b t

val update_on_exception : 'a t -> f:(On_exception.t -> On_exception.t) -> 'a t
