open! Core
open! Async_kernel

type t = string [@@deriving sexp_of]

let local_storage_key =
  Univ_map.Key.create ~name:"async_rpc_kernel_metadata" [%sexp_of: t]
;;

let get () = Async_kernel.Async_kernel_scheduler.find_local local_storage_key

module Private = struct
  let set metadata ctx = Execution_context.with_local ctx local_storage_key metadata
end
