open! Core
open! Async_kernel

module V1 = struct
  type t = string [@@deriving bin_io ~localize, globalize, sexp_of, equal]

  let of_string s = s
  let to_string s = s
  let truncate = String.prefix
  let bin_read_t__local = Bin_prot.Read.bin_read_string__local
end

module V2 = struct
  module Key = struct
    module T = struct
      type t = int [@@deriving bin_io ~localize, globalize, sexp, compare, hash, equal]
    end

    include T
    include Comparable.Make (T)
    include Hashable.Make (T)

    let of_int i = i
    let default_for_legacy = 0
  end

  module Payload = struct
    type t = string [@@deriving bin_io ~localize, globalize, sexp, equal]

    let to_string s = s
    let of_string_maybe_truncate s = s
  end

  type t = (Key.t * Payload.t) list
  [@@deriving bin_io ~localize, globalize, sexp_of, equal]

  let empty = []
  let add t ~key ~payload = List.Assoc.add t ~equal:Key.equal key payload
  let find t ~key = List.Assoc.find t ~equal:Key.equal key
  let singleton key payload = [ key, payload ]
  let singleton__local key payload = exclave_ [ key, payload ]
  let of_map map = Map.to_alist map ~key_order:`Increasing
  let of_table = Hashtbl.to_alist
  let to_alist t = t
  let to_alist__local t = t

  let of_alist alist =
    match Key.Map.of_alist alist with
    | `Ok map -> Ok (of_map map)
    | `Duplicate_key key -> Or_error.error_s [%message "Duplicate key" (key : Key.t)]
  ;;

  let to_v1 v2 = find v2 ~key:Key.default_for_legacy
  let of_v1 v1 = singleton Key.default_for_legacy v1
  let of_v1__local v1 = exclave_ singleton__local Key.default_for_legacy v1

  let truncate_payloads t max_length =
    List.map t ~f:(fun (key, value) -> key, String.prefix value max_length)
  ;;

  let bin_read_t__local =
    Bin_prot.Read.bin_read_list__local
      (Bin_prot.Read.bin_read_pair__local
         Bin_prot.Read.bin_read_int__local
         Bin_prot.Read.bin_read_string__local)
  ;;
end

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
