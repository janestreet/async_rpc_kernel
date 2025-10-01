(** Internal to [Async_rpc_kernel]. See [Rpc.Decscription]. *)

open! Core

type t =
  { global_ name : string
  ; version : int
  }
[@@deriving bin_io, equal ~localize, compare ~localize, hash, sexp_of, globalize]

include Comparable.S with type t := t
include Hashable.S with type t := t

val summarize : t list -> Int.Set.t String.Map.t
val of_alist : (string * int) list -> t list
val to_alist : t list -> (string * int) list

module Stable : sig
  module V1 : sig
    type nonrec t = t
    [@@deriving
      compare ~localize
      , equal ~localize
      , globalize
      , sexp
      , bin_io ~localize
      , hash
      , stable_witness]

    val bin_read_t__local : t Bin_prot.Read.reader__local

    include
      Stable_comparable.V1
      with type t := t
       and type comparator_witness := comparator_witness
  end
end
