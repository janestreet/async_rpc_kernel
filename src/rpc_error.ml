open Core
open! Import
include Async_rpc_kernel_types.Rpc_error

let to_error t ~rpc_description ~connection_description ~connection_close_started =
  to_error
    t
    ~rpc_description
    ~connection_description
    ~peek_connection_close_started_callback:(fun () ->
      Async_kernel.Deferred.peek connection_close_started |> Or_null.of_option)
;;
