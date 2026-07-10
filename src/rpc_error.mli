open! Core
open! Async_kernel
open! Import
include module type of Async_rpc_kernel_types.Rpc_error

val to_error
  :  t
  -> rpc_description:Description.t
  -> connection_description:Info.t @ portable
  -> connection_close_started:Info.Portable.t Deferred.t
  -> Error.t @ portable
