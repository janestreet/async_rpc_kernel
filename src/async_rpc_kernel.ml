module Rpc = Rpc
module Versioned_rpc = Versioned_rpc
module Menu = Async_rpc_kernel_types.Menu
module Persistent_connection = Persistent_connection
module Pipe_transport = Pipe_transport
module Rpc_error = Rpc_error
module Rpc_result = Rpc_result
module Rpc_shapes = Async_rpc_kernel_types.Rpc_shapes
module Tracing_event = Async_rpc_kernel_types.Tracing_event
module Or_not_authorized = Or_not_authorized
module Close_reason = Async_rpc_kernel_types.Close_reason

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
  module Rpc_metadata : module type of Rpc_metadata_private = Rpc_metadata_private

  module Connection : Connection_intf.S_private with type t = Rpc.Connection.t =
    Connection

  module Handshake_error = Async_rpc_kernel_types.Handshake_error
  module Header = Async_rpc_kernel_types.Header
  module Protocol = Async_rpc_kernel_types.Protocol
  module Protocol_local_readers = Async_rpc_kernel_types.Protocol_local_readers
  module Transport = Transport
  module Util = Util
  module Version_dependent_feature = Async_rpc_kernel_types.Version_dependent_feature
  module Writer_with_length = Writer_with_length

  let default_handshake_timeout = Connection.default_handshake_timeout
end
