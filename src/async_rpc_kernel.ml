module Rpc = Rpc
module Versioned_rpc = Versioned_rpc
module Menu = Menu
module Persistent_connection = Persistent_connection
module Pipe_transport = Pipe_transport
module Rpc_error = Rpc_error
module Rpc_result = Rpc_result
module Rpc_shapes = Rpc_shapes
module Tracing_event = Tracing_event
module Or_not_authorized = Or_not_authorized

open struct
  module Rpc_metadata_private = Rpc_metadata
end

module Rpc_metadata :
  module type of Rpc_metadata with module Private := Rpc_metadata.Private =
  Rpc_metadata

module Async_rpc_kernel_stable = struct
  module Rpc = Rpc.Stable
end

module Async_rpc_kernel_private = struct
  module Rpc_metadata : module type of Rpc_metadata_private with type t = Rpc_metadata.t =
    Rpc_metadata_private

  module Connection : Connection_intf.S_private with type t = Rpc.Connection.t =
    Connection

  module Protocol = Protocol
  module Transport = Transport
  module Util = Util

  let default_handshake_timeout = Connection.default_handshake_timeout
end
