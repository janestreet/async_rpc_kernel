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

    type query = unit [@@deriving bin_io]

    let%expect_test _ =
      print_endline [%bin_digest: query];
      [%expect {| 86ba5df747eec837f0b391dd49f33f9e |}]
    ;;

    type response = (Description.Stable.V1.t * Rpc_shapes.Stable.Just_digests.V1.t) list
    [@@deriving bin_io, sexp_of]

    let%expect_test _ =
      print_endline [%bin_digest: response];
      [%expect {| bfa1a67e3782922212d253c848e49da8 |}]
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
type t =
  { descriptions : Description.t array
      (* strictly sorted. One thing we don't do but might want is to make equal names phys_equal *)
  ; digests : Rpc_shapes.Just_digests.t array option
      (* None means [Unknown] everywhere. Otherwise elements correspond to [descriptions]. *)
  }

let supported_rpcs (t : t) = Array.to_list t.descriptions

(* Returns inclusive (lower, upper) bounds for the interval of indexes which corresponds
   to rpcs with this name less than or equal to this version. If no such rpcs exist:
   Error `No_versions if rpcs exist with this name but larger versions
   Error `No_rpcs if no rpcs exist with this name
*)
let versions_range t ~rpc_name ~max_version =
  let max_version = Option.value max_version ~default:Int.max_value in
  let compare (d : Description.t) x = [%compare: string] d.name x in
  match Array.binary_search t.descriptions ~compare `First_equal_to rpc_name with
  | None -> Error `No_rpcs
  | Some lb ->
    (match
       Array.binary_search_segmented
         t.descriptions
         ~segment_of:(fun d ->
           if [%compare_local: Description.t] d { name = rpc_name; version = max_version }
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

let index t description =
  match
    Array.binary_search_segmented
      t.descriptions
      ~segment_of:(fun d ->
        if [%compare_local: Description.t] d description <= 0 then `Left else `Right)
      `Last_on_left
  with
  | None -> None
  | Some i ->
    if [%compare_local.equal: Description.t] t.descriptions.(i) description
    then Some i
    else None
;;

let mem t description =
  match index t description with
  | Some (_ : int) -> true
  | None -> false
;;

let shape_digests t description =
  match index t description with
  | None -> None
  | Some i ->
    (match t.digests with
     | Some digests -> Some digests.(i)
     | None -> Some Unknown)
;;

(* This function is a bit ugly. We want to (a) reasonably avoid allocations, (b) be fast
   in the case where max of the set is the same as or near to max of the versions in the
   menu. *)
let highest_available_version t ~rpc_name ~from_set =
  (* If nothing exists in [from_set] we still need to know whether there exist rpcs with
     the given [rpc_name] *)
  let max_version = Set.max_elt from_set in
  match versions_range t ~rpc_name ~max_version with
  | Error `No_rpcs -> Error `No_rpcs_with_this_name
  | Error `No_versions -> Error `Some_versions_but_none_match
  | Ok (lb_in_descriptions, ub_in_descriptions) ->
    (* We search for versions before this test so that we can produce the
       no_rpcs/some_versions error *)
    if Set.is_empty from_set
    then Error `Some_versions_but_none_match
    else (
      let descriptions = t.descriptions in
      let rec search description_index set_index set_value =
        match compare descriptions.(description_index).version set_value with
        | 0 -> Ok set_value
        | c when c < 0 ->
          let set_index = set_index - 1 in
          if set_index < 0
          then Error `Some_versions_but_none_match
          else
            search
              description_index
              set_index
              (Set.nth from_set set_index |> Option.value_exn)
        | _ (* > 0 *) ->
          let description_index = description_index - 1 in
          if description_index < lb_in_descriptions
          then Error `Some_versions_but_none_match
          else search description_index set_index set_value
      in
      let set_index = Set.length from_set in
      (* We know there exists at least one element in the set since [from_set] is not
         empty in this branch *)
      search ub_in_descriptions set_index (Option.value_exn max_version))
;;

let has_some_versions t ~rpc_name =
  match highest_available_version t ~rpc_name ~from_set:Int.Set.empty with
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

(* we want a sexp that is useful for debugging. We produce a sexp where the
   entries look like:
   [((name <rpc-name>)(kind <e.g. One_way>)(versions (1 2 3)))]
   When one name has multiple shapes, it gets multiple entries (and the version lists
   don't overlap). *)
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
  match highest_available_version callee_menu ~rpc_name ~from_set:caller_versions with
  | Ok _ as ok -> ok
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
