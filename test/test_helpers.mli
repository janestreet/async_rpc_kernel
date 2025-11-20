(** Shared helpers for tests. *)

open! Core
open! Async
module Header = Async_rpc_kernel.Async_rpc_kernel_private.Connection.For_testing.Header

val rpc : (Bigstring.t, Bigstring.t) Rpc.Rpc.t
val rpc_v2 : (Bigstring.t, Bigstring.t) Rpc.Rpc.t
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

  (* Print raw protocol bytes and an interpretation as one of the given bin shapes *)
  val print_messages : t -> Bin_shape.t Nonempty_list.t -> unit

  val print_messages_bidirectional
    :  Bin_shape.t Nonempty_list.t
    -> s_to_c:t
    -> c_to_s:t
    -> unit
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
  :  ?time_source:[> read ] Time_source.T1.t
  -> ?provide_rpc_shapes:bool
  -> ?server_implementations:unit Rpc.Implementation.t list
  -> ?client_implementations:unit Rpc.Implementation.t list
  -> unit
  -> server_header:Header.t
  -> client_header:Header.t
  -> f:
       (client:Rpc.Connection.t
        -> server:Rpc.Connection.t
        -> s_to_c:Tap.t
        -> c_to_s:Tap.t
        -> 'a Deferred.t)
  -> 'a Deferred.t

(* [?server_heartbeat_foo] are after the labeled arguments since they default to the value
   of [heartbeat_foo]. *)
val setup_server_and_client_connection
  :  heartbeat_timeout:Time_ns.Span.t
  -> heartbeat_every:Time_ns.Span.t
  -> heartbeat_timeout_style:Async_rpc_kernel.Rpc.Connection.Heartbeat_timeout_style.t
  -> ?server_heartbeat_timeout:Time_ns.Span.t
  -> ?server_heartbeat_every:Time_ns.Span.t
  -> ?server_heartbeat_timeout_style:
       Async_rpc_kernel.Rpc.Connection.Heartbeat_timeout_style.t
  -> unit
  -> ([ `Server of read_write Synchronous_time_source.T1.t * Rpc.Connection.t ]
     * [ `Client of read_write Synchronous_time_source.T1.t * Rpc.Connection.t ])
       Deferred.t

module Payload : sig
  type t = int array [@@deriving bin_io]

  val get_random_size : unit -> int
  val create : unit -> t
end
