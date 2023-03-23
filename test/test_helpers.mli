(** Shared helpers for tests. *)

open! Core
open! Async
module Header = Async_rpc_kernel.Async_rpc_kernel_private.Connection.For_testing.Header

val rpc : (Bigstring.t, Bigstring.t) Rpc.Rpc.t
val rpc_tag : string
val rpc_version : int
val one_way_rpc : Bigstring.t Rpc.One_way.t
val pipe_rpc : (Bigstring.t, Bigstring.t, Error.t) Rpc.Pipe_rpc.t
val state_rpc : (Bigstring.t, Bigstring.t, Bigstring.t, Error.t) Rpc.State_rpc.t

val with_local_connection
  :  ?lift_implementation:(unit Rpc.Implementation.t -> unit Rpc.Implementation.t)
  -> header:Header.t
  -> f:(Rpc.Connection.t -> unit Deferred.t)
  -> unit
  -> unit Deferred.t

val with_rpc_server_connection
  :  server_header:Header.t
  -> client_header:Header.t
  -> f:(sender:Rpc.Connection.t -> receiver:Rpc.Connection.t -> unit Deferred.t)
  -> unit Deferred.t
