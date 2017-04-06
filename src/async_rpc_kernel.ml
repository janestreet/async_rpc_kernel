module Rpc                   = Rpc
module Versioned_rpc         = Versioned_rpc
module Persistent_connection = Persistent_connection
module Pipe_transport        = Pipe_transport

module Async_rpc_kernel_private = struct
  module Protocol = Protocol
  let default_handshake_timeout = Connection.default_handshake_timeout
end
