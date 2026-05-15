open! Core
open! Async_kernel
include Async_rpc_kernel_types.Rpc_metadata

let local_storage_key =
  Univ_map.Key.create
    ~name:"async_rpc_kernel_query_metadata_legacy_payload"
    [%sexp_of: V2.Payload.t]
;;

let get_from_context_for_legacy () =
  Async_kernel.Async_kernel_scheduler.find_local local_storage_key
;;

module Private = struct
  let set_context_for_legacy metadata ctx =
    Execution_context.with_local ctx local_storage_key metadata
  ;;
end
