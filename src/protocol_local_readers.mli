open! Core
include module type of Protocol

module Sexp : sig
  include module type of Sexp

  val bin_read_t__local : t Bin_prot.Read.reader__local
end

module Message : sig
  include module type of Message

  (* Currently, this will allocate when reading [Close_reason], [Metadata], or
     [Metadata_v2], or if the ['a] reader allocates *)
  val bin_read_t__local : ('a, 'a t) Bin_prot.Read.reader1__local
  val bin_read_nat0_t__local : Nat0.t t Bin_prot.Read.reader__local
end
