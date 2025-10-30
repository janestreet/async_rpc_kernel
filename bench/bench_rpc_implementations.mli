open! Core
open! Async

val one_way_rpc : string Rpc.One_way.t
val echo_rpc : (string, string) Rpc.Rpc.t
val error_rpc : (Sexp.t, unit) Rpc.Rpc.t
val pipe_rpc : (int, int, unit) Rpc.Pipe_rpc.t

module Connection_pair : sig
  type t =
    { server : Rpc.Connection.t
    ; client : Rpc.Connection.t
    }
end

val create_connection_pair_with_pipe_transport : unit -> Connection_pair.t Deferred.t
val create_connection_pair_with_async_transport : unit -> Connection_pair.t Deferred.t
