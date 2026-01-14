open! Core

module Closer = struct
  type t =
    | By_unknown
    | By_local
    | By_remote
  [@@deriving sexp_of]
end

module Protocol = struct
  module Kind = struct
    module T = struct
      type t =
        | Other of Flexible_sexp.Variant.Stable.Other.V1.t
        | Unspecified
        | Connection_limit_reached
        | Connection_validation_failed
      [@@deriving sexp, compare, variants, globalize]
    end

    include T
    include Flexible_sexp.Variant.Stable.Make.V1 (T) ()

    let%expect_test "Kind is stable unless a new variant is added" =
      print_endline [%bin_digest: t];
      [%expect {| 832b40ae394f2851da8ba67b3339b429 |}]
    ;;
  end

  type t =
    { kind : Kind.t
    ; debug_info : Info_with_local_bin_io.t option
    ; user_reason : Info_with_local_bin_io.t option
    }
  [@@deriving sexp_of, compare]

  let create ?(kind = Kind.Unspecified) ?debug_info ?user_reason () =
    { debug_info; user_reason; kind }
  ;;

  module Binable = struct
    type t =
      { kind : Sexp.t
      ; debug_info : Info_with_local_bin_io.t option
      ; user_reason : Info_with_local_bin_io.t option
      }
    [@@deriving bin_io ~localize, globalize, sexp_of]
  end

  let info_of_t t =
    let { kind; debug_info; user_reason } = t in
    let body_info =
      match debug_info, user_reason with
      | None, None -> Info.of_string ""
      | Some info, None | None, Some info -> info
      | Some debug_info, Some user_reason ->
        Info.create_s
          [%message
            ""
              (debug_info : Info_with_local_bin_io.t)
              (user_reason : Info_with_local_bin_io.t)]
    in
    match kind with
    | Unspecified -> body_info
    | kind ->
      Info.create_s
        [%message "" ~_:(kind : Kind.t) ~_:(body_info : Info_with_local_bin_io.t)]
  ;;

  let sexp_of_t t = Info.sexp_of_t (info_of_t t)

  let binable_of_t ({ kind; debug_info; user_reason } : t) : Binable.t =
    { kind = Kind.sexp_of_t kind; debug_info; user_reason }
  ;;

  let t_of_binable ({ kind; debug_info; user_reason } : Binable.t) : t =
    { kind = Kind.t_of_sexp kind; debug_info; user_reason }
  ;;
end

type t =
  { closer : Closer.t
  ; reason : Protocol.t
  ; connection_description : Info.t
  }

let aux_info closer reason =
  let info = Protocol.info_of_t reason in
  match closer with
  | Closer.By_unknown -> info
  | By_local -> Info.tag info ~tag:"Connection closed by local side:"
  | By_remote -> Info.tag info ~tag:"Connection closed by remote side:"
;;

let info_of_t { closer; reason; connection_description } =
  Info.of_list
    [ aux_info closer reason
    ; Info.tag connection_description ~tag:"connection_description"
    ]
;;

let sexp_of_t t = Info.sexp_of_t (info_of_t t)

module For_testing = struct
  module Kind_with_extra_variant = struct
    type t =
      | Other of Flexible_sexp.Variant.Stable.Other.V1.t
      | Unspecified
      | Connection_limit_reached
      | Connection_validation_failed
      | Extra_variant
    [@@deriving sexp, compare, variants, globalize]
  end
end

let%expect_test "kind serializes flexibly" =
  let kind = For_testing.Kind_with_extra_variant.sexp_of_t Extra_variant in
  let binable =
    { Protocol.Binable.kind
    ; debug_info = Some (Info.of_string "debug info")
    ; user_reason = Some (Info.of_string "user reason")
    }
  in
  let deserialized_close_reason : Protocol.t = Protocol.t_of_binable binable in
  print_s [%sexp (deserialized_close_reason : Protocol.t)];
  [%expect {| (Extra_variant ((debug_info "debug info") (user_reason "user reason"))) |}];
  (match deserialized_close_reason.kind with
   | Other other ->
     print_s
       [%message
         "Flexible_sexp Other variant:" (other : Flexible_sexp.Variant.Stable.Other.V1.t)]
   | unexpected_variant ->
     failwithf
       "Unexpected variant: %s"
       (Sexp.to_string (Protocol.Kind.sexp_of_t unexpected_variant))
       ());
  [%expect {| ("Flexible_sexp Other variant:" (other Extra_variant)) |}]
;;
