open! Core
open! Async_kernel

include module type of struct
  include Persistent_connection_kernel
end

module Rpc_conn : Closable with type t = Rpc.Connection.t
module Versioned_rpc_conn : Closable with type t = Versioned_rpc.Connection_with_menu.t

module Rpc :
  S
  with type conn = Rpc.Connection.t
   and type t = Persistent_connection_kernel.Make(Rpc_conn).t

module Versioned_rpc :
  S
  with type conn = Versioned_rpc.Connection_with_menu.t
   and type t = Persistent_connection_kernel.Make(Versioned_rpc_conn).t
