open! Core
open! Async_kernel

type t = string [@@deriving sexp_of]

let local_storage_key =
  Univ_map.Key.create ~name:"async_rpc_kernel_metadata" [%sexp_of: t]
;;

let get () = Async_kernel.Async_kernel_scheduler.find_local local_storage_key

module Private = struct
  let with_metadata metadata ~f =
    Async_kernel.Async_kernel_scheduler.with_local local_storage_key metadata ~f
  ;;
end
