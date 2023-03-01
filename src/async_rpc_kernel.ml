module Rpc = Rpc
module Versioned_rpc = Versioned_rpc
module Persistent_connection = Persistent_connection
module Pipe_transport = Pipe_transport
module Rpc_error = Rpc_error
module Rpc_result = Rpc_result
module Rpc_shapes = Rpc_shapes

module Rpc_metadata :
  module type of Rpc_metadata with module Private := Rpc_metadata.Private =
  Rpc_metadata

module Async_rpc_kernel_stable = struct
  module Rpc = Rpc.Stable
end

module Async_rpc_kernel_private = struct
  module Connection = Connection
  module Protocol = Protocol
  module Transport = Transport

  let default_handshake_timeout = Connection.default_handshake_timeout
end
