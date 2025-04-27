(** Internal to [Async_rpc_kernel]. See [Rpc.Implementation]. *)

open! Core
open! Async_kernel
open Protocol

module Expert : sig
  module Responder : sig
    type t =
      { query_id : Query_id.t
      ; impl_menu_index : Protocol.Impl_menu_index.t
      ; writer : Protocol_writer.t
      ; mutable responded : bool
      }
    [@@deriving sexp_of]

    val create : Query_id.t -> Protocol.Impl_menu_index.t -> Protocol_writer.t -> t
  end

  type implementation_result =
    | Replied
    | Delayed_response of unit Deferred.t
end

module F : sig
  type ('connection_state, 'query, 'init, 'update) streaming_impl =
    | Pipe of
        ('connection_state
         -> 'query
         -> ('init * 'update Pipe.Reader.t, 'init) Result.t Or_not_authorized.t Or_error.t
              Deferred.t)
    | Direct of
        ('connection_state
         -> 'query
         -> 'update Implementation_types.Direct_stream_writer.t
         -> ('init, 'init) Result.t Or_not_authorized.t Or_error.t Deferred.t)

  type ('connection_state, 'query, 'init, 'update) streaming_rpc =
    { bin_query_reader : 'query Bin_prot.Type_class.reader
    ; bin_init_writer : 'init Bin_prot.Type_class.writer
    ; bin_update_writer : 'update Bin_prot.Type_class.writer
        (* 'init can be an error or an initial state *)
    ; impl : ('connection_state, 'query, 'init, 'update) streaming_impl
    ; error_mode : 'init Implementation_mode.Error_mode.t
    ; leave_open_on_exception : bool
    ; here : Source_code_position.t
    }

  type 'connection_state t =
    | One_way :
        'msg Bin_prot.Type_class.reader
        * ('connection_state -> 'msg -> unit Or_not_authorized.t Or_error.t Deferred.t)
        * Source_code_position.t
        -> 'connection_state t
    | One_way_expert :
        ('connection_state
         -> Bigstring.t
         -> pos:int
         -> len:int
         -> unit Or_not_authorized.t Or_error.t Deferred.t)
        -> 'connection_state t
    | Rpc :
        'query Bin_prot.Type_class.reader
        * 'response Bin_prot.Type_class.writer
        * ('connection_state -> 'query -> 'result)
        * 'response Implementation_mode.Error_mode.t
        * ('response, 'result) Implementation_mode.Result_mode.t
        * Source_code_position.t
        -> 'connection_state t
    | Rpc_expert :
        ('connection_state
         -> Expert.Responder.t
         -> Bigstring.t
         -> pos:int
         -> len:int
         -> 'result)
        * (Expert.implementation_result, 'result) Implementation_mode.Result_mode.t
        -> 'connection_state t
    | Streaming_rpc :
        ('connection_state, 'query, 'init, 'update) streaming_rpc
        -> 'connection_state t
    | Menu_rpc : Menu.Stable.V3.response Lazy.t -> 'connection_state t

  val lift : 'a t -> f:('b -> 'a Or_not_authorized.t Or_error.t) -> 'b t

  val lift_deferred
    :  'a t
    -> f:('b -> 'a Or_not_authorized.t Or_error.t Deferred.t)
    -> 'b t
end

type 'connection_state t =
  { tag : Rpc_tag.t
  ; version : int
  ; f : 'connection_state F.t
  ; shapes : (Rpc_shapes.t * Rpc_shapes.Just_digests.t) Lazy.t
  ; on_exception : On_exception.t option
  }
[@@deriving sexp_of]

val description : _ t -> Description.t
val shapes : _ t -> Rpc_shapes.t
val digests : _ t -> Rpc_shapes.Just_digests.t
val lift : 'a t -> f:('b -> 'a) -> 'b t
val lift_deferred : 'a t -> f:('b -> 'a Deferred.t) -> 'b t
val try_lift : 'a t -> f:('b -> 'a Or_error.t) -> 'b t
val try_lift_deferred : 'a t -> f:('b -> 'a Or_error.t Deferred.t) -> 'b t
val with_authorization : 'a t -> f:('b -> 'a Or_not_authorized.t) -> 'b t

val with_authorization_deferred
  :  'authorized_connection_state t
  -> f:('connection_state -> 'authorized_connection_state Or_not_authorized.t Deferred.t)
  -> 'connection_state t

val update_on_exception
  :  'connection_state t
  -> f:(On_exception.t option -> On_exception.t)
  -> 'connection_state t
