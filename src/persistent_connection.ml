open! Core
open! Async_kernel
include Persistent_connection_kernel

module Versioned_rpc_conn = struct
  type t = Versioned_rpc.Connection_with_menu.t

  let rpc_connection = Versioned_rpc.Connection_with_menu.connection
  let close t = Rpc.Connection.close (rpc_connection t)
  let is_closed t = Rpc.Connection.is_closed (rpc_connection t)
  let close_finished t = Rpc.Connection.close_finished (rpc_connection t)
end

module Versioned_rpc = Make (Versioned_rpc_conn)

module Rpc_conn = struct
  type t = Rpc.Connection.t

  let close t = Rpc.Connection.close t
  let is_closed t = Rpc.Connection.is_closed t
  let close_finished t = Rpc.Connection.close_finished t
end

module Rpc = Make (Rpc_conn)
