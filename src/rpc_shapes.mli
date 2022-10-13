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

module Stable : sig
  module V1 : sig
    type nonrec t = t [@@deriving bin_io, sexp]
  end
end
