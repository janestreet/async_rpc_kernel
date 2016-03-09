(** Internal to [Async_rpc_kernel].  See [Rpc.Decscription]. *)

open! Core_kernel.Std

type t =
  { name    : string
  ; version : int
  }
[@@deriving compare, sexp_of, bin_io]

include Comparable.S with type t := t
include Hashable  .S with type t := t

module Stable : sig
  module V1 : sig
    type nonrec t = t [@@deriving compare, sexp, bin_io]
  end
end
