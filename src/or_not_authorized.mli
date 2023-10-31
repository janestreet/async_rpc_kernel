open! Core
open Async_kernel

type 'a t =
  | Authorized of 'a
  | Not_authorized of Error.t

val bind : 'a t -> f:('a -> 'b t) -> 'b t
val map : 'a t -> f:('a -> 'b) -> 'b t
val bind_deferred : 'a t -> f:('a -> 'b t Deferred.t) -> 'b t Deferred.t
val map_deferred : 'a t -> f:('a -> 'b Deferred.t) -> 'b t Deferred.t
val lift_deferred : 'a Deferred.t t -> 'a t Deferred.t
