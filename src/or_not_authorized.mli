open! Core
open Async_kernel
include module type of Async_rpc_kernel_types.Or_not_authorized

val lift_deferred : 'a Deferred.t t -> 'a t Deferred.t

module Deferred : Monad.S with type 'a t := 'a t Deferred.t
