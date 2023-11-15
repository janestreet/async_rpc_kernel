(** Shared helpers for tests. *)

open! Core
open! Async
module Header = Async_rpc_kernel.Async_rpc_kernel_private.Connection.For_testing.Header

val rpc : (Bigstring.t, Bigstring.t) Rpc.Rpc.t
val one_way_rpc : Bigstring.t Rpc.One_way.t
val pipe_rpc : (Bigstring.t, Bigstring.t, Error.t) Rpc.Pipe_rpc.t
val state_rpc : (Bigstring.t, Bigstring.t, Bigstring.t, Error.t) Rpc.State_rpc.t
val sort_rpc : (int array, int array) Rpc.Rpc.t
val server_identification : Bigstring.t
val client_identification : Bigstring.t

(** Records a copy of bytes passed through a connection and exposes functions for tests to
    provide a way to interpret the bytes and pretty print them *)
module Tap : sig
  type t

  (* Print raw protocol bytes and an interpretation as RPC connection headers *)
  val print_header : t -> unit
  val print_headers : s_to_c:t -> c_to_s:t -> unit

  (* Print raw protocol bytes and an interpretation as the given bin shape *)
  val print_messages : t -> Bin_shape.t -> unit
  val print_messages_bidirectional : Bin_shape.t -> s_to_c:t -> c_to_s:t -> unit
end

val with_circular_connection
  :  ?lift_implementation:(unit Rpc.Implementation.t -> unit Rpc.Implementation.t)
  -> header:Header.t
  -> f:(Rpc.Connection.t -> Tap.t -> unit Deferred.t)
  -> unit
  -> unit Deferred.t

(** Set up a tcp server to server rpcs and connect to it. Here all things [server]
    correspond to the end which calls [accept]. Currently in our tests we always send rpcs
    from the client to the server. *)
val with_rpc_server_connection
  :  server_header:Header.t
  -> client_header:Header.t
  -> f:
       (client:Rpc.Connection.t
        -> server:Rpc.Connection.t
        -> s_to_c:Tap.t
        -> c_to_s:Tap.t
        -> 'a Deferred.t)
  -> 'a Deferred.t
