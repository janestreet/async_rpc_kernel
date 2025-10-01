open! Core
include module type of Protocol

module Sexp : sig
  include module type of Sexp

  val bin_read_t__local : t Bin_prot.Read.reader__local
end

module Message : sig
  include module type of Message

  (* Currently, this will allocate when reading [Close_reason], [Close_reason_v2],
     [Metadata], or [Metadata_v2], or if the ['a] reader allocates *)
  val bin_read_t__local : ('a, 'a t) Bin_prot.Read.reader1__local
  val bin_read_nat0_t__local : Nat0.t t Bin_prot.Read.reader__local
end

module Stream_query : sig
  include module type of Stream_query

  val bin_read_needs_length__local : ('a, 'a needs_length) Bin_prot.Read.reader1__local
  val bin_read_nat0_t__local : Nat0.t needs_length Bin_prot.Read.reader__local
end

module Stream_initial_message : sig
  include module type of Stream_initial_message

  val bin_read_t__local
    : ('response, 'error, ('response, 'error) t) Bin_prot.Read.reader2__local
end

module Stream_response_data : sig
  include module type of Stream_response_data

  val bin_read_needs_length__local : ('a, 'a needs_length) Bin_prot.Read.reader1__local
  val bin_read_nat0_t__local : Nat0.t needs_length Bin_prot.Read.reader__local
end
