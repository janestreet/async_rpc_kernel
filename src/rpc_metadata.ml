open! Core
open! Async_kernel

module V1 = struct
  type t = string [@@deriving bin_io ~localize, globalize, sexp_of]

  let of_string s = s
  let to_string s = s
  let truncate = String.prefix
  let bin_read_t__local = Bin_prot.Read.bin_read_string__local
end

let local_storage_key =
  Univ_map.Key.create ~name:"async_rpc_kernel_metadata" [%sexp_of: V1.t]
;;

let get () = Async_kernel.Async_kernel_scheduler.find_local local_storage_key

module Private = struct
  let set metadata ctx = Execution_context.with_local ctx local_storage_key metadata
end
