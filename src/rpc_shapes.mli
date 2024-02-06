(** A serializable representation of the bin_shape(s) of an RPC. For a regular RPC, this
    is a simple query/response pair. *)

open! Core

type t =
  | Rpc of
      { query : Bin_prot.Shape.t
      ; response : Bin_prot.Shape.t
      }
  | One_way of { msg : Bin_prot.Shape.t }
  | Streaming_rpc of
      { query : Bin_prot.Shape.t
      ; initial_response : Bin_prot.Shape.t
      ; update_response : Bin_prot.Shape.t
      ; error : Bin_prot.Shape.t
      }
  | Unknown
[@@deriving sexp_of]

module Just_digests : sig
  type t =
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
  [@@deriving sexp_of, variants]

  (** True if the variants are the same, e.g. both are [Rpc _] *)
  val same_kind : t -> t -> bool

  module Strict_comparison : sig
    (** Total order on [t]. [Unknown] is equal only to [Unknown]. You might not want this
        kind of comparison for things other than e.g. making a [Map.t] *)

    type nonrec t = t [@@deriving compare]
  end
end

val eval_to_digest : t -> Just_digests.t

module Stable : sig
  module V1 : sig
    type nonrec t = t [@@deriving bin_io, sexp]
  end

  module Just_digests : sig
    module V1 : sig
      type t = Just_digests.t [@@deriving bin_io, compare, equal, hash, sexp]
    end
  end
end
