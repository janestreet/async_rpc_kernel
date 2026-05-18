module Stable = struct
  open! Core.Core_stable

  (* We can't make [Bin_prot.Shape.t] binable directly because that would introduce a
     circular dependency, so we make it [Binable.Of_sexpable_with_uuid] here instead. This
     allows us to send the shape over the wire as part of the
     [Versioned_rpc.Shape_menu.t]. [Bin_prot.Shape.Canonical.t], though exposed and
     therefore binable, is not a suitable alternative because it has an exponential
     expansion. *)
  module Shape = struct
    module V1 = struct
      module T = struct
        include Bin_prot.Shape.Stable.V1

        let caller_identity =
          Bin_prot.Shape.Uuid.of_string "0aa53549-ad88-4b4f-abc7-1f8453e7aa11"
        ;;
      end

      include T
      include Binable.Of_sexpable.V2 (T)
    end
  end

  module V1 = struct
    type t =
      | Rpc of
          { query : Shape.V1.t
          ; response : Shape.V1.t
          }
      | One_way of { msg : Shape.V1.t }
      | Streaming_rpc of
          { query : Shape.V1.t
          ; initial_response : Shape.V1.t
          ; update_response : Shape.V1.t
          ; error : Shape.V1.t
          }
      | Unknown
    [@@deriving bin_io, sexp]
  end

  module Just_digests = struct
    module Digest = struct
      module V1 = struct
        type t = Bin_shape.Digest.t [@@deriving compare ~localize, globalize, sexp]

        let%template[@mode local] equal t1 t2 = ([%compare.equal: t] [@mode local]) t1 t2
        let hash_fold_t s t = Core.Md5.hash_fold_t s (Bin_shape.Digest.to_md5 t)

        include
          Binable.Of_binable.V2
            (Bin_prot.Md5.Stable.V1)
            (struct
              type nonrec t = t

              let to_binable = Bin_shape.Digest.to_md5
              let of_binable = Bin_shape.Digest.of_md5

              let caller_identity =
                Bin_prot.Shape.Uuid.of_string "d8669bfc-1cdf-11ee-9283-aa42dc4c5cc4"
              ;;
            end)

        let bin_size_t__local t =
          bin_size_md5__local (Bin_shape.Digest.to_md5_local t) [@nontail]
        ;;

        let bin_write_t__local buf ~pos t =
          Bin_prot.Write.bin_write_md5__local
            buf
            ~pos
            (Bin_shape.Digest.to_md5_local t) [@nontail]
        ;;
      end
    end

    module V1 = struct
      type t =
        | Rpc of
            { query : Digest.V1.t
            ; response : Digest.V1.t
            }
        | One_way of { msg : Digest.V1.t }
        | Streaming_rpc of
            { query : Digest.V1.t
            ; initial_response : Digest.V1.t
            ; update_response : Digest.V1.t
            ; error : Digest.V1.t
            }
        | Unknown
      [@@deriving
        bin_io ~localize, equal ~localize, compare ~localize, globalize, hash, sexp]

      let bin_read_t__local buf ~pos_ref = exclave_
        let open Bin_prot.Read in
        match bin_read_int_8bit buf ~pos_ref with
        | 0 ->
          let query = bin_read_md5__local buf ~pos_ref |> Bin_shape.Digest.of_md5_local in
          let response =
            bin_read_md5__local buf ~pos_ref |> Bin_shape.Digest.of_md5_local
          in
          Rpc { query; response }
        | 1 ->
          let msg = bin_read_md5__local buf ~pos_ref |> Bin_shape.Digest.of_md5_local in
          One_way { msg }
        | 2 ->
          let query = bin_read_md5__local buf ~pos_ref |> Bin_shape.Digest.of_md5_local in
          let initial_response =
            bin_read_md5__local buf ~pos_ref |> Bin_shape.Digest.of_md5_local
          in
          let update_response =
            bin_read_md5__local buf ~pos_ref |> Bin_shape.Digest.of_md5_local
          in
          let error = bin_read_md5__local buf ~pos_ref |> Bin_shape.Digest.of_md5_local in
          Streaming_rpc { query; initial_response; update_response; error }
        | 3 -> Unknown
        | _ ->
          Bin_prot.Common.raise_read_error
            (Bin_prot.Common.ReadError.Sum_tag "Rpc_shapes.Just_digests local read")
            !pos_ref
      ;;
    end
  end
end

open! Core

module Shape = struct
  type t = Bin_prot.Shape.t

  (* An unstable sexper that's useful for expect tests, printing out the shape digests
     instead of the shapes themselves. *)
  let sexp_of_t t = [%sexp (Bin_prot.Shape.eval_to_digest_string t : string)]
end

type t = Stable.V1.t =
  | Rpc of
      { query : Shape.t
      ; response : Shape.t
      }
  | One_way of { msg : Shape.t }
  | Streaming_rpc of
      { query : Shape.t
      ; initial_response : Shape.t
      ; update_response : Shape.t
      ; error : Shape.t
      }
  | Unknown
[@@deriving sexp_of]

module Just_digests = struct
  type t = Stable.Just_digests.V1.t =
    | Rpc of
        { query : Bin_shape.Digest.t
        ; response : Bin_shape.Digest.t
        }
    | One_way of { msg : Bin_shape.Digest.t }
    | Streaming_rpc of
        { query : Bin_shape.Digest.t
        ; initial_response : Bin_shape.Digest.t
        ; update_response : Bin_shape.Digest.t
        ; error : Bin_shape.Digest.t
        }
    | Unknown
  [@@deriving globalize, sexp_of, compare ~localize, variants]

  module Strict_comparison = struct
    type nonrec t = t [@@deriving compare ~localize]
  end

  let same_kind = Comparable.lift [%equal: int] ~f:Variants.to_rank
end

let eval_to_digest (t : t) : Just_digests.t =
  let digest shape = Bin_prot.Shape.eval_to_digest shape in
  match t with
  | Rpc { query; response } -> Rpc { query = digest query; response = digest response }
  | One_way { msg } -> One_way { msg = digest msg }
  | Streaming_rpc { query; initial_response; update_response; error } ->
    Streaming_rpc
      { query = digest query
      ; initial_response = digest initial_response
      ; update_response = digest update_response
      ; error = digest error
      }
  | Unknown -> Unknown
;;
