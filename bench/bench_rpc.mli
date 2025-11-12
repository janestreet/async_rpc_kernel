open! Core
open Async

module type Config = sig
  val transport_name : string

  val create_connection_pair
    :  unit
    -> Bench_rpc_implementations.Connection_pair.t Deferred.t
end

module Make_benches (C : Config) : sig end
