module Stable = struct
  open Core.Core_stable

  (* We must be very careful with the versions here: menus are serialized/deserialized a
     lot. V1 is the only version supported by the legacy [Versioned_rpc.Menu] rpc. V2 is
     the only version supported in the protocol v3 handshake. *)

  module V1 = struct
    let version = 1

    type query = unit [@@deriving bin_io]

    let%expect_test _ =
      print_endline [%bin_digest: query];
      [%expect {| 86ba5df747eec837f0b391dd49f33f9e |}]
    ;;

    type response = (string * int) list [@@deriving bin_io, sexp_of]

    let%expect_test _ =
      print_endline [%bin_digest: response];
      [%expect {| 4c1e50c93b38c2ad0554cbd929bef3ac |}]
    ;;

    let response_of_model = Description.to_alist
  end

  module V2 = struct
    let version = 2

    type query = unit [@@deriving bin_io ~localize]

    let%expect_test _ =
      print_endline [%bin_digest: query];
      [%expect {| 86ba5df747eec837f0b391dd49f33f9e |}]
    ;;

    type response = (Description.Stable.V1.t * Rpc_shapes.Stable.Just_digests.V1.t) list
    [@@deriving bin_io ~localize, globalize, sexp_of]

    let%expect_test _ =
      print_endline [%bin_digest: response];
      [%expect {| bfa1a67e3782922212d253c848e49da8 |}]
    ;;

    let bin_read_response__local buf ~pos_ref = exclave_
      let open Bin_prot.Read in
      let bin_read_el =
        bin_read_pair__local
          Description.Stable.V1.bin_read_t__local
          Rpc_shapes.Stable.Just_digests.V1.bin_read_t__local
      in
      bin_read_list__local bin_read_el buf ~pos_ref
    ;;
  end

  module V3 = struct
    let version = 3

    type query = unit [@@deriving bin_io ~localize]

    let%expect_test _ =
      print_endline [%bin_digest: query];
      [%expect {| 86ba5df747eec837f0b391dd49f33f9e |}]
    ;;

    type response =
      { descriptions : Description.Stable.V1.t array
      ; digests : Rpc_shapes.Stable.Just_digests.V1.t array option
      }
    [@@deriving bin_io ~localize, globalize, sexp_of]

    let bin_read_response__local buf ~pos_ref = exclave_
      let open Bin_prot.Read in
      let descriptions =
        bin_read_array__local Description.Stable.V1.bin_read_t buf ~pos_ref
      in
      let digests =
        bin_read_option__local
          (bin_read_array__local Rpc_shapes.Stable.Just_digests.V1.bin_read_t)
          buf
          ~pos_ref
      in
      { descriptions; digests }
    ;;

    let%expect_test _ =
      print_endline [%bin_digest: response];
      [%expect {| 59ecd3468dc49f69d0993360e80aff6d |}]
    ;;

    let to_v2_response response =
      let open Core.Option.Let_syntax in
      let%bind digests = response.digests in
      Core.Array.zip response.descriptions digests >>| Core.Array.to_list
    ;;
  end
end

open Core

let version_menu_rpc_name = "__Versioned_rpc.Menu"

(* The main thing we want to do with the menu is search for implementations when we
   [dispatch_multi]. This is the case we optimize for and, in particular, we optimize for
   the case where server and client both have the latest or nearly-latest version. We also
   want to avoid allocating when doing this search of the menu as some users expect that.

   To keep things simple, we just have and search a sorted array of descriptions (note
   that descriptions sort first by name so the different versions of an rpc are
   contiguous) instead of some multi-level scheme. This means a bit less pointer-chasing
   than with a Map.t but is otherwise straightforward. We keep digests separately because
   they are rarely used. *)
type t = Stable.V3.response =
  { descriptions : Description.t array
      (* strictly sorted. One thing we don't do but might want is to make equal names
         phys_equal *)
  ; digests : Rpc_shapes.Just_digests.t array option
  (* None means [Unknown] everywhere. Otherwise elements correspond to [descriptions]. *)
  }
[@@deriving globalize]

let supported_rpcs (t : t) = Array.to_list t.descriptions

(* Returns inclusive (lower, upper) bounds for the interval of indexes which corresponds
   to rpcs with this name less than or equal to this version. If no such rpcs exist: Error
   `No_versions if rpcs exist with this name but larger versions Error `No_rpcs if no rpcs
   exist with this name
*)
let%template[@zero_alloc] versions_range t ~rpc_name ~(local_ max_version) = exclave_
  let max_version = (Option.value [@mode local]) max_version ~default:Int.max_value in
  let compare (d : Description.t) x = [%compare: string] d.name x in
  match
    (Array.binary_search [@zero_alloc assume])
      t.descriptions
      ~compare
      `First_equal_to
      rpc_name
  with
  | None -> Error `No_rpcs
  | Some lb ->
    (match
       (Array.binary_search_segmented [@zero_alloc assume])
         t.descriptions
         ~segment_of:(fun d ->
           if ([%compare: Description.t] [@mode local])
                d
                { name = rpc_name; version = max_version }
              <= 0
           then `Left
           else `Right)
         `Last_on_left
     with
     | None -> Error `No_versions
     | Some ub -> if ub >= lb then Ok (lb, ub) else Error `No_versions)
;;

let supported_versions t ~rpc_name =
  match versions_range t ~rpc_name ~max_version:None with
  | Error (`No_rpcs | `No_versions) -> Int.Set.empty
  | Ok (lb, ub) ->
    Int.Set.of_increasing_iterator_unchecked
      ~len:(ub - lb + 1)
      ~f:(fun i -> t.descriptions.(lb + i).version)
;;

let get t index = exclave_
  if index >= Array.length t.descriptions
  then None
  else Some (Modes.Global.wrap t.descriptions.(index))
;;

let%template index__local t ~(local_ tag) ~(local_ version) = exclave_
  let reference_tag = tag in
  let reference_version = version in
  match
    (Array.binary_search_segmented [@zero_alloc assume])
      (* We can assume that binary_search_segmented does not alloc because the
         [segment_of] parameter to it is zero_alloc. Additionally, microbenchmarks show
         that the change that provoked this (invoking RPCs by menu rank) did not introduce
         an alloc in the dispatch path. *)
      t.descriptions
      ~segment_of:(fun [@zero_alloc] d ->
        let { Description.name; version } = d in
        let cmp =
          ([%compare: string * int] [@mode local])
            (name, version)
            (reference_tag, reference_version)
        in
        if cmp <= 0 then `Left else `Right)
      `Last_on_left
  with
  | None -> None
  | Some i ->
    let { Description.name; version } = t.descriptions.(i) in
    if ([%compare.equal: string] [@mode local]) name reference_tag
       && ([%compare.equal: int] [@mode local]) version reference_version
    then Some i
    else None
;;

(* Even though [description] is annotated local_, the [name] field on [Description.t] is
   forced to be [global_], thus requiring a separate local version of [index]. *)
let index t (local_ description) = exclave_
  let { Description.name = tag; version } = description in
  index__local t ~tag ~version
;;

let mem t description =
  match index t description with
  | Some (_ : int) -> true
  | None -> false
;;

let includes_shape_digests t = Option.is_some t.digests

let shape_digests t description =
  match index t description with
  | None -> None
  | Some i ->
    (match t.digests with
     | Some digests -> Some digests.(i)
     | None -> Some Unknown)
;;

(* This function is a bit ugly. We want to (a) avoid allocating, (b) be fast in the case
   where max of the set is the same as or near to max of the versions in the menu. *)
let%template[@zero_alloc] highest_available_version t ~rpc_name ~from_sorted_array
  = exclave_
  (* If nothing exists in [from_sorted_array] we still need to know whether there exist
     rpcs with the given [rpc_name] *)
  let local_ max_version =
    if Array.is_empty from_sorted_array
    then None
    else Some ((Array.last_exn [@zero_alloc assume]) from_sorted_array)
  in
  match versions_range t ~rpc_name ~max_version with
  | Error `No_rpcs -> Error `No_rpcs_with_this_name
  | Error `No_versions -> Error `Some_versions_but_none_match
  | Ok (lb_in_descriptions, ub_in_descriptions) ->
    (* We search for versions before this test so that we can produce the
       no_rpcs/some_versions error *)
    if Array.is_empty from_sorted_array
    then Error `Some_versions_but_none_match
    else (
      let descriptions = t.descriptions in
      let[@zero_alloc] rec local_ search description_index arr_index arr_value = exclave_
        match compare descriptions.(description_index).version arr_value with
        | 0 -> Ok arr_value
        | c when c < 0 ->
          let arr_index = arr_index - 1 in
          if arr_index < 0
          then Error `Some_versions_but_none_match
          else search description_index arr_index (Array.get from_sorted_array arr_index)
        | _ (*=> 0 *) ->
          let description_index = description_index - 1 in
          if description_index < lb_in_descriptions
          then Error `Some_versions_but_none_match
          else search description_index arr_index arr_value
      in
      let arr_index = Array.length from_sorted_array in
      (* We know there exists at least one element in the set since [from_set] is not
         empty in this branch *)
      search
        ub_in_descriptions
        arr_index
        ((Option.value_exn [@mode local]) max_version) [@nontail])
;;

let has_some_versions t ~rpc_name =
  match highest_available_version t ~rpc_name ~from_sorted_array:[||] with
  | Ok _ | Error `Some_versions_but_none_match -> true
  | Error `No_rpcs_with_this_name -> false
;;

let ensure_no_duplicates descriptions =
  if Array.is_sorted_strictly descriptions ~compare:[%compare: Description.t]
  then ()
  else
    raise_s
      [%message
        "Invalid rpc menu: duplicate entry"
          ~entry:
            (Array.find_consecutive_duplicate descriptions ~equal:[%equal: Description.t]
             |> Option.value_exn
             |> fst
             : Description.t)]
;;

let of_supported_rpcs descriptions ~rpc_shapes:`Unknown =
  let descriptions = Array.of_list descriptions in
  Array.sort descriptions ~compare:[%compare: Description.t];
  ensure_no_duplicates descriptions;
  { descriptions; digests = None }
;;

let of_supported_rpcs_and_shapes descriptions_and_shapes =
  let descriptions, digests =
    List.sort
      descriptions_and_shapes
      ~compare:(Comparable.lift [%compare: Description.t] ~f:fst)
    |> Array.of_list
    |> Array.unzip
  in
  ensure_no_duplicates descriptions;
  { descriptions; digests = Some digests }
;;

let of_v1_response (v1_response : Stable.V1.response) : t =
  let descriptions =
    Array.of_list_map v1_response ~f:(fun (name, version) ->
      { Description.name; version })
  in
  Array.sort descriptions ~compare:[%compare: Description.t];
  ensure_no_duplicates descriptions;
  { descriptions; digests = None }
;;

let of_v2_response (v2_response : Stable.V2.response) : t =
  if List.is_sorted v2_response ~compare:[%compare: Description.t * _]
  then (
    let descriptions = Array.of_list_map v2_response ~f:fst in
    let digests = Array.of_list_map v2_response ~f:snd in
    ensure_no_duplicates descriptions;
    { descriptions; digests = Some digests })
  else (
    let items = Array.of_list v2_response in
    Array.sort items ~compare:[%compare: Description.t * _];
    let descriptions, digests = Array.unzip items in
    ensure_no_duplicates descriptions;
    { descriptions; digests = Some digests })
;;

(* we want a sexp that is useful for debugging. We produce a sexp where the entries look
   like: [((name <rpc-name>)(kind <e.g. One_way>)(versions (1 2 3)))] When one name has
   multiple shapes, it gets multiple entries (and the version lists don't overlap). *)
let sexp_of_t { descriptions; digests } =
  let with_digest =
    match digests with
    | Some d -> Array.zip_exn descriptions d |> Array.to_list
    | None ->
      Array.to_list descriptions
      |> List.map ~f:(fun desc -> desc, Rpc_shapes.Just_digests.Unknown)
  in
  let grouped =
    List.group with_digest ~break:(fun (d1, s1) (d2, s2) ->
      String.( <> ) d1.name d2.name || not (Rpc_shapes.Just_digests.same_kind s1 s2))
  in
  List.map grouped ~f:(function
    | [] -> assert false
    | ({ name; version = _ }, first_digest) :: _ as items ->
      let kind = Rpc_shapes.Just_digests.Variants.to_name first_digest in
      let versions =
        List.map items ~f:(fun ({ name = _; version }, (_ : Rpc_shapes.Just_digests.t)) ->
          version)
      in
      [%sexp { name : string; kind : string; versions : int list }])
  |> Sexp.List
;;

module With_digests_in_sexp = struct
  type nonrec t = t

  let sexp_of_t t =
    let rpc_names =
      t
      |> supported_rpcs
      |> List.map ~f:(fun { name; version = _ } -> name)
      |> List.dedup_and_sort ~compare:String.compare
    in
    List.map rpc_names ~f:(fun name ->
      match versions_range t ~rpc_name:name ~max_version:None with
      | Error (`No_versions | `No_rpcs) ->
        raise_s
          [%message
            "Bug in Async_rpc_kernel.With_digests_in_sexp.sexp_of_t. Expected to find at \
             least one such description"]
      | Ok (lb, ub) ->
        let versions =
          List.init
            (ub - lb + 1)
            ~f:(fun i ->
              let i = lb + i in
              let digest =
                match t.digests with
                | Some d -> d.(i)
                | None -> Unknown
              in
              let version = t.descriptions.(i).version in
              [%sexp { version : int; digest : Rpc_shapes.Just_digests.t }])
        in
        [%sexp { name : string; versions : Sexp.t list }])
    |> Sexp.List
  ;;
end

(* This depends on the sexp printer, hence it lives down here *)
let highest_shared_version ~rpc_name ~callee_menu ~caller_versions =
  let from_sorted_array = Set.to_array caller_versions in
  match highest_available_version callee_menu ~rpc_name ~from_sorted_array with
  | Ok version -> Ok version
  | Error `Some_versions_but_none_match ->
    error_s
      [%message
        "Caller and callee share no common versions for rpc"
          rpc_name
          (caller_versions : Int.Set.t)
          ~callee_versions:(supported_versions callee_menu ~rpc_name : Int.Set.t)]
  | Error `No_rpcs_with_this_name ->
    error_s
      [%message
        "Callee does not know this rpc"
          (rpc_name : string)
          (caller_versions : Int.Set.t)
          (callee_menu : t)]
;;

let check_digests_consistent (t1 : t) (t2 : t) =
  match Option.both t1.digests t2.digests with
  | None ->
    Or_error.error_s
      [%message
        "Menu missing digests"
          (t1.digests : Rpc_shapes.Just_digests.t array option)
          (t2.digests : Rpc_shapes.Just_digests.t array option)]
  | Some (t1_digests, t2_digests) ->
    (* We know that both arrays are sorted which is not verified by the types but is done
       in the constructors. *)
    let rec aux_check_digests_consistent t1_index t2_index =
      if [%equal: int] t1_index (Array.length t1_digests)
         || [%equal: int] t2_index (Array.length t2_digests)
      then Ok ()
      else (
        match
          Ordering.of_int
            ([%compare: Description.t]
               t1.descriptions.(t1_index)
               t2.descriptions.(t2_index))
        with
        | Less -> aux_check_digests_consistent (t1_index + 1) t2_index
        | Greater -> aux_check_digests_consistent t1_index (t2_index + 1)
        | Equal ->
          let description = t1.descriptions.(t1_index) in
          let t1_digest = t1_digests.(t1_index) in
          let t2_digest = t2_digests.(t2_index) in
          (* If either digest is [Unknown] then we cannot check consistency properly so we
             return an error. *)
          (match t1_digest, t2_digest with
           | Unknown, (_ : Rpc_shapes.Just_digests.t)
           | (_ : Rpc_shapes.Just_digests.t), Unknown ->
             Or_error.error_s
               [%message
                 "Menu contains [Unknown] digest"
                   (description : Description.t)
                   (t1_digest : Rpc_shapes.Just_digests.t)
                   (t2_digest : Rpc_shapes.Just_digests.t)]
           | t1_digest, t2_digest ->
             if [%compare.equal: Rpc_shapes.Just_digests.Strict_comparison.t]
                  t1_digest
                  t2_digest
             then aux_check_digests_consistent (t1_index + 1) (t2_index + 1)
             else
               Or_error.error_s
                 [%message
                   "Found digest mismatch"
                     (description : Description.t)
                     (t1_digest : Rpc_shapes.Just_digests.t)
                     (t2_digest : Rpc_shapes.Just_digests.t)]))
    in
    aux_check_digests_consistent 0 0
;;
