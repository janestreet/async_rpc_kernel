open! Core

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
      type t = int
      [@@deriving
        bin_io ~localize, globalize, sexp, compare ~localize, hash, equal ~localize]
    end

    include T

    include%template Comparable.Make [@modality portable] (T)
    include%template Hashable.Make [@modality portable] (T)

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
  let of_map map = Map.to_alist map ~key_order:`Increasing
  let of_table = Hashtbl.to_alist

  let of_alist alist =
    match Key.Map.of_alist alist with
    | `Ok map -> Ok (of_map map)
    | `Duplicate_key key -> Or_error.error_s [%message "Duplicate key" (key : Key.t)]
  ;;

  let add t ~key ~payload = List.Assoc.add t ~equal:Key.equal key payload

  [%%template
  [@@@alloc.default a @ l = (heap_global, stack_local)]

  let find t ~key =
    (List.Assoc.find_or_null [@mode l])
      t
      ~equal:(Key.equal [@mode l])
      key [@exclave_if_stack a]
  ;;

  let singleton key payload = [ key, payload ] [@exclave_if_stack a]
  let to_alist t = t [@exclave_if_stack a]
  let to_v1 v2 = (find [@alloc a]) v2 ~key:Key.default_for_legacy [@exclave_if_stack a]
  let of_v1 v1 = (singleton [@alloc a]) Key.default_for_legacy v1 [@exclave_if_stack a]]

  let truncate_payloads t max_length =
    List.map t ~f:(fun (key, value) -> key, String.prefix value max_length)
  ;;

  let bin_read_t__local =
    (* This top-level declaration is necessary to avoid allocating this as a closure at
       the [bin_read_list__local] callsite *)
    let bin_read_int_string_pair__local buf ~pos_ref = exclave_
      Bin_prot.Read.bin_read_pair__local
        Bin_prot.Read.bin_read_int__local
        Bin_prot.Read.bin_read_string__local
        buf
        ~pos_ref
    in
    fun buf ~pos_ref -> exclave_
      Bin_prot.Read.bin_read_list__local bin_read_int_string_pair__local buf ~pos_ref
  ;;
end
