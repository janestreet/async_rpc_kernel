open! Core
open! Async_kernel
open! Import
include module type of Async_rpc_kernel_types.Rpc_result

val try_with
  :  here:Source_code_position.t
  -> (unit -> 'a t Deferred.t) @ local
  -> location:string
  -> on_background_exception:On_exception.Background_monitor_rest.t option
  -> 'a t Deferred.t

val or_error
  :  rpc_description:Description.t
  -> connection_description:Info.t @ portable
  -> connection_close_started:Info.Portable.t Deferred.t
  -> 'a t
  -> 'a Or_error.t
