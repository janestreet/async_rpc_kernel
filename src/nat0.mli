(** The bin-prot representation of lengths of strings, arrays, etc. *)

type t = Bin_prot.Nat0.t [@@deriving bin_io]

(** fails on negative values *)
val of_int_exn : int -> t

module Option : sig
  type outer := t
  type t : immediate [@@deriving bin_io ~localize]

  val bin_read_t__local : t Bin_prot.Read.reader__local [@@zero_alloc opt arity 2]
  val globalize : local_ t -> t

  include Core.Immediate_option.S with type t := t and type value := outer
end
