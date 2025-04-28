include struct
  open Async_rpc_kernel
  module Async_rpc_kernel_private = Async_rpc_kernel_private
  module Menu = Menu
  module Rpc = Rpc
  module Tracing_event = Tracing_event
end

include struct
  open Async_rpc_kernel_private
  module Connection = Connection
  module Protocol = Protocol
  module Protocol_local_readers = Protocol_local_readers
  module Writer_with_length = Writer_with_length
end
