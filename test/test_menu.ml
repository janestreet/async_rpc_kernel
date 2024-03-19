open! Core
open Async_rpc_kernel
module Description = Rpc.Description

let generate_rpc_name =
  Quickcheck.Generator.(
    union [ String.gen_nonempty; of_list [ "foo"; "bar"; "baz"; "FOO"; "Foo" ] ])
;;

module V1_or_v2 = struct
  let digest_generator =
    [ [%bin_shape: unit]
    ; [%bin_shape: int]
    ; [%bin_shape: string]
    ; [%bin_shape: int * int]
    ; [%bin_shape: string * int]
    ; [%bin_shape: string * string]
    ; [%bin_shape: int * int * int]
    ; [%bin_shape: int * int * int * int]
    ]
    |> List.map ~f:Bin_shape.eval_to_digest
    |> Quickcheck.Generator.of_list
  ;;

  let just_digests_generator =
    let open Quickcheck.Let_syntax in
    let open Rpc_shapes.Just_digests in
    Quickcheck.Generator.union
      [ return Unknown
      ; (let%map query = digest_generator
         and response = digest_generator in
         Rpc { query; response })
      ; (let%map msg = digest_generator in
         One_way { msg })
      ; (let%map query = digest_generator
         and initial_response = digest_generator
         and update_response = digest_generator
         and error = digest_generator in
         Streaming_rpc { query; initial_response; update_response; error })
      ]
  ;;

  let v2_generator =
    let open Quickcheck.Let_syntax in
    let%map_open list =
      Quickcheck.Generator.list
        (let%map name = generate_rpc_name
         and version_delta = Int.gen_incl 1 10
         and just_digests = just_digests_generator in
         name, version_delta, just_digests)
    and sort = bool in
    let response =
      let versions = String.Table.create () in
      List.map list ~f:(fun (name, delta, digest) ->
        let version = Hashtbl.find versions name |> Option.value ~default:(-2) in
        let version = version + delta in
        Hashtbl.set versions ~key:name ~data:version;
        { Description.name; version }, digest)
    in
    let response =
      if sort then List.sort response ~compare:[%compare: Description.t * _] else response
    in
    `V2 response
  ;;

  let v1_generator =
    Quickcheck.Generator.map v2_generator ~f:(fun (`V2 list) ->
      let v1 =
        List.map
          list
          ~f:(fun ({ Description.name; version }, (_ : Rpc_shapes.Just_digests.t)) ->
          name, version)
      in
      `V1 v1)
  ;;

  type t =
    [ `V1 of (string * int) list
    | `V2 of (Description.t * Rpc_shapes.Just_digests.t) list
    ]
  [@@deriving sexp_of]

  let generator = Quickcheck.Generator.union [ v1_generator; v2_generator ]
end

module type Menu_intf = sig
  type t [@@deriving sexp_of]

  val of_v1_or_v2 : V1_or_v2.t -> t
  val supported_rpcs : t -> Description.t list
  val supported_versions : t -> rpc_name:string -> Int.Set.t
  val mem : t -> Description.t -> bool
  val shape_digests : t -> Description.t -> Rpc_shapes.Just_digests.t option

  val highest_available_version
    :  t
    -> rpc_name:string
    -> from_set:Int.Set.t
    -> (int, [ `Some_versions_but_none_match | `No_rpcs_with_this_name ]) Result.t
end

module Menu : Menu_intf = struct
  include Menu

  let of_v1_or_v2 = function
    | `V1 v1 -> of_v1_response v1
    | `V2 v2 -> of_v2_response v2
  ;;
end

module Reference_menu : Menu_intf = struct
  type t = (Description.t * Rpc_shapes.Just_digests.t) list [@@deriving sexp_of]

  let of_v1_or_v2 = function
    | `V2 v2 -> v2
    | `V1 v1 ->
      List.map v1 ~f:(fun (name, version) ->
        { Description.name; version }, Rpc_shapes.Just_digests.Unknown)
  ;;

  let supported_rpcs (t : t) = t |> List.map ~f:fst

  let supported_versions (t : t) ~rpc_name =
    List.filter t ~f:(fun (desc, _) -> String.equal rpc_name desc.name)
    |> List.map ~f:(fun (desc, _) -> desc.version)
    |> Int.Set.of_list
  ;;

  let mem (t : t) desc =
    List.mem
      (supported_rpcs t)
      ([%globalize: Description.t] desc)
      ~equal:Description.equal
  ;;

  let shape_digests t desc =
    List.Assoc.find t ([%globalize: Description.t] desc) ~equal:[%equal: Description.t]
  ;;

  let highest_available_version (t : t) ~rpc_name ~from_set =
    let available = supported_versions t ~rpc_name in
    if Set.is_empty available
    then Error `No_rpcs_with_this_name
    else (
      match Set.max_elt (Set.inter available from_set) with
      | None -> Error `Some_versions_but_none_match
      | Some x -> Ok x)
  ;;
end

let do_test' (type t) sexp_of_t (compare : t -> t -> int) ~gen ~seed ~f =
  let n = ref 500 in
  Quickcheck.test
    ~sizes:(Sequence.cycle_list_exn [ 20; 5; 10; 1; 20 ])
    ~seed:(`Deterministic (Int.to_string seed))
    ~sexp_of:[%sexp_of: _ * V1_or_v2.t * _]
    (let%map.Core.Quickcheck.Generator input = V1_or_v2.generator
     and state = Int.gen_uniform_incl 0 Int.max_value
     and extra = gen in
     extra, input, state)
    ~f:(fun (gen, input, state) ->
      let menu : t =
        f gen (module Menu : Menu_intf) input (Random.State.make [| state |])
      in
      let expect =
        f gen (module Reference_menu : Menu_intf) input (Random.State.make [| state |])
      in
      [%test_result: t] menu ~expect;
      n := !n - 1;
      if !n = 0
      then
        print_s
          [%message
            "500th example" ~menu:(Menu.of_v1_or_v2 input : Menu.t) ~result:(menu : t)])
;;

let do_test sexp_of compare ~seed ~f =
  do_test' ~gen:Unit.quickcheck_generator sexp_of compare ~seed ~f:(fun () -> f)
;;

let%expect_test "supported_rpcs" =
  do_test
    [%sexp_of: Description.Set.t]
    [%compare: Description.Set.t]
    ~seed:12760
    ~f:(fun (module M) x _state ->
    x |> M.of_v1_or_v2 |> M.supported_rpcs |> Description.Set.of_list);
  [%expect
    {|
    ("500th example"
     (menu
      (((name 2) (kind One_way) (versions (3)))
       ((name 2) (kind Rpc) (versions (11))) ((name 4) (kind Rpc) (versions (0)))
       ((name 8) (kind Rpc) (versions (4)))
       ((name :) (kind Streaming_rpc) (versions (0)))
       ((name A) (kind Streaming_rpc) (versions (6)))
       ((name D) (kind Unknown) (versions (8)))
       ((name FOO) (kind Streaming_rpc) (versions (1)))
       ((name Foo) (kind One_way) (versions (8 13)))
       ((name Foo) (kind Rpc) (versions (14)))
       ((name K) (kind Unknown) (versions (3)))
       ((name S) (kind Unknown) (versions (2)))
       ((name W) (kind Streaming_rpc) (versions (5)))
       ((name Y) (kind Rpc) (versions (-1)))
       ((name baz) (kind Streaming_rpc) (versions (8)))
       ((name foo) (kind Unknown) (versions (-1)))
       ((name p) (kind Rpc) (versions (7)))
       ((name "\189") (kind Rpc) (versions (4)))))
     (result
      (((name 2) (version 3)) ((name 2) (version 11)) ((name 4) (version 0))
       ((name 8) (version 4)) ((name :) (version 0)) ((name A) (version 6))
       ((name D) (version 8)) ((name FOO) (version 1)) ((name Foo) (version 8))
       ((name Foo) (version 13)) ((name Foo) (version 14)) ((name K) (version 3))
       ((name S) (version 2)) ((name W) (version 5)) ((name Y) (version -1))
       ((name baz) (version 8)) ((name foo) (version -1)) ((name p) (version 7))
       ((name "\189") (version 4)))))
    |}]
;;

let%expect_test "sexp_of doesn't raise" =
  do_test [%sexp_of: _] [%compare: _] ~seed:15531 ~f:(fun (module M) x _state ->
    x |> M.of_v1_or_v2 |> M.sexp_of_t);
  [%expect
    {|
    ("500th example"
     (menu (((name "qnCm[AABZ\172Oul\187a0R") (kind Unknown) (versions (8)))))
     (result _))
    |}]
;;

let choose_rpc_name (v1_or_v2 : V1_or_v2.t) state =
  with_return (fun { return } ->
    let nth list =
      let len = List.length list in
      let n = Random.State.int state (len + 1) in
      if n = len then return "not-in-menu" else List.nth_exn list n
    in
    match v1_or_v2 with
    | `V1 v1 -> fst (nth v1)
    | `V2 v2 -> (fst (nth v2)).name)
;;

let%expect_test "supported_versions" =
  do_test
    [%sexp_of: Int.Set.t]
    [%compare: Int.Set.t]
    ~seed:6751
    ~f:(fun (module M) x state ->
    x |> M.of_v1_or_v2 |> M.supported_versions ~rpc_name:(choose_rpc_name x state));
  [%expect
    {|
    ("500th example"
     (menu
      (((name 8) (kind Rpc) (versions (4)))
       ((name "JP\255") (kind One_way) (versions (8)))
       ((name "\226J\000ZPrj") (kind One_way) (versions (-1)))))
     (result (-1)))
    |}]
;;

let%expect_test "mem" =
  do_test
    [%sexp_of: bool * (string * int)]
    [%compare: bool * _]
    ~seed:13668
    ~f:(fun (module M) x state ->
    let t = M.of_v1_or_v2 x in
    let name = choose_rpc_name x state in
    let versions = t |> M.supported_versions ~rpc_name:name in
    let version =
      if Random.State.bool state || Set.is_empty versions
      then
        (* arbitrary value that may/may not be in the menu, or before/after all elts *)
        8
      else
        Set.nth versions (Random.State.int state (Set.length versions))
        |> Option.value_exn
    in
    M.mem t { name; version }, (name, version));
  [%expect
    {|
    ("500th example"
     (menu
      (((name ,) (kind Streaming_rpc) (versions (4)))
       ((name 7) (kind Streaming_rpc) (versions (0)))
       ((name :) (kind Unknown) (versions (-1)))
       ((name FOO) (kind One_way) (versions (8 11)))
       ((name FOO) (kind Streaming_rpc) (versions (12)))
       ((name FOO) (kind Unknown) (versions (18)))
       ((name FOO) (kind One_way) (versions (26)))
       ((name T) (kind Streaming_rpc) (versions (4)))
       ((name V) (kind One_way) (versions (1)))
       ((name b) (kind Unknown) (versions (0)))
       ((name f) (kind One_way) (versions (4)))
       ((name foo) (kind One_way) (versions (-1)))
       ((name j) (kind One_way) (versions (3)))
       ((name jb) (kind Unknown) (versions (0)))
       ((name q) (kind One_way) (versions (0)))
       ((name s) (kind Streaming_rpc) (versions (-1)))
       ((name x) (kind Streaming_rpc) (versions (1)))))
     (result (true (, 4))))
    |}]
;;

let%expect_test "shape_digests" =
  do_test
    [%sexp_of: Rpc_shapes.Just_digests.t option * (string * int)]
    [%compare: Rpc_shapes.Just_digests.Strict_comparison.t option * _]
    ~seed:8069
    ~f:(fun (module M) x state ->
    let t = M.of_v1_or_v2 x in
    let name = choose_rpc_name x state in
    let versions = t |> M.supported_versions ~rpc_name:name in
    let version =
      if Random.State.bool state || Set.is_empty versions
      then
        (* arbitrary value that may/may not be in the menu, or before/after all elts *)
        8
      else
        Set.nth versions (Random.State.int state (Set.length versions))
        |> Option.value_exn
    in
    M.shape_digests t { name; version }, (name, version));
  [%expect
    {|
    ("500th example"
     (menu
      (((name /) (kind Streaming_rpc) (versions (8)))
       ((name 7) (kind Unknown) (versions (1)))
       ((name 9w) (kind Unknown) (versions (2)))
       ((name Foo) (kind One_way) (versions (-1)))
       ((name Foo) (kind Streaming_rpc) (versions (2)))
       ((name Foo) (kind Unknown) (versions (10)))
       ((name Foo) (kind Streaming_rpc) (versions (17)))
       ((name K) (kind One_way) (versions (2)))
       ((name Q) (kind Unknown) (versions (7)))
       ((name bar) (kind Streaming_rpc) (versions (0)))
       ((name c) (kind Streaming_rpc) (versions (3)))
       ((name foo) (kind One_way) (versions (7)))
       ((name foo) (kind Streaming_rpc) (versions (14)))
       ((name foo) (kind One_way) (versions (16)))
       ((name k) (kind Rpc) (versions (3)))))
     (result (() (Q 8))))
    |}]
;;

let%expect_test "highest_available_version" =
  let open struct
    type x = (int, [ `Some_versions_but_none_match | `No_rpcs_with_this_name ]) Result.t
    [@@deriving compare, sexp]
  end in
  do_test'
    [%sexp_of: x * (string * Int.Set.t)]
    [%compare: x * _]
    ~seed:8636
    ~gen:(Int.Set.quickcheck_generator (Int.gen_incl (-2) 20))
    ~f:(fun from_set (module M) x state ->
      let t = M.of_v1_or_v2 x in
      let rpc_name = choose_rpc_name x state in
      M.highest_available_version t ~rpc_name ~from_set, (rpc_name, from_set));
  [%expect
    {|
    ("500th example"
     (menu
      (((name FOO) (kind Unknown) (versions (6)))
       ((name Mi) (kind Rpc) (versions (2)))))
     (result ((Ok 2) (Mi (2)))))
    |}]
;;

let%expect_test "highest_available_version 2" =
  Core.Quickcheck.test
    ~sizes:(Sequence.cycle_list_exn [ 1; 10; 50; 75; 100; 200; 300; 400; 500; 750; 1000 ])
    ~seed:(`Deterministic "fjdsakgfjdil")
    ~sexp_of:[%sexp_of: (string * int) list * (string * Int.Set.t)]
    (let open Core.Quickcheck.Let_syntax in
     let ver_set = Int.Set.quickcheck_generator Int.quickcheck_generator in
     let%map_open foo_versions = ver_set >>| Set.to_list
     and bar_versions = ver_set >>| Set.to_list
     and challenge = ver_set
     and challenge_name = of_list [ "foo"; "bar" ] in
     ( List.map foo_versions ~f:(fun v -> "foo", v)
       @ List.map bar_versions ~f:(fun v -> "bar", v)
     , (challenge_name, challenge) ))
    ~f:(fun (v1, (rpc_name, from_set)) ->
      let t = Menu.of_v1_or_v2 (`V1 v1) in
      let t_ref = Reference_menu.of_v1_or_v2 (`V1 v1) in
      let res = Menu.highest_available_version t ~rpc_name ~from_set in
      let expect = Reference_menu.highest_available_version t_ref ~rpc_name ~from_set in
      [%test_result:
        (int, [ `No_rpcs_with_this_name | `Some_versions_but_none_match ]) Result.t]
        res
        ~expect)
;;

let%expect_test "highest_available_version disjoint" =
  let one_run () =
    let rpc, query =
      let i = ref 0 in
      List.init 10_000 ~f:(fun _ ->
        i := !i + 1 + Random.int 5;
        !i)
      |> List.partition_tf ~f:(fun _ -> Random.bool ())
    in
    let t = Menu.of_v1_or_v2 (`V1 (List.map rpc ~f:(fun v -> "foo", v))) in
    [%test_result:
      (int, [ `No_rpcs_with_this_name | `Some_versions_but_none_match ]) Result.t]
      (Menu.highest_available_version t ~rpc_name:"foo" ~from_set:(Int.Set.of_list query))
      ~expect:(Error `Some_versions_but_none_match)
  in
  List.init 5 ~f:Fn.id |> List.iter ~f:(fun (_ : int) -> one_run ())
;;
