(** Internal to [Async_rpc_kernel]. *)

open! Core_kernel.Std
open! Async_kernel.Std

type 'a t = 'a Protocol.Rpc_result.t

val uncaught_exn : location:string -> exn -> 'a t

val bin_io_exn : location:string -> exn -> 'a t

val try_with
  :  ?run:[ `Now | `Schedule ]
  -> location:string
  -> (unit -> 'a t Deferred.t)
  -> 'a t Deferred.t

val or_error
  : rpc_tag:Protocol.Rpc_tag.t
  -> rpc_version:int
  -> connection_description:Info.t
  -> 'a t
  -> 'a Or_error.t
