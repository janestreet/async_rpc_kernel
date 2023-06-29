(** Internal to [Async_rpc_kernel].  See [Rpc.Decscription]. *)

open! Core

type t =
  { name : string [@global]
  ; version : int
  }
[@@deriving bin_io, equal, compare, hash, sexp_of, globalize]

include Comparable.S with type t := t
include Hashable.S with type t := t

val summarize : t list -> Int.Set.t String.Map.t

module Stable : sig
  module V1 : sig
    type nonrec t = t [@@deriving compare, equal, sexp, bin_io, hash]
  end
end
