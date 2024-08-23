open! Core
open! Async_kernel

type 'a t = 'a Protocol.Rpc_result.t

val uncaught_exn : location:string -> exn -> 'a t
val bin_io_exn : location:string -> exn -> 'a t
val authorization_failure : location:string -> exn -> 'a t
val lift_error : location:string -> exn -> 'a t

val try_with
  :  here:Source_code_position.t
  -> local_ (unit -> 'a t Deferred.t)
  -> Description.t
  -> location:string
  -> on_background_exception:On_exception.t
  -> close_connection_monitor:Monitor.t
  -> 'a t Deferred.t

val or_error
  :  rpc_description:Description.t
  -> connection_description:Info.t
  -> connection_close_started:Info.t Deferred.t
  -> 'a t
  -> 'a Or_error.t
