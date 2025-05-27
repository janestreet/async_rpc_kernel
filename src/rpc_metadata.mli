(** Metadata is arbitrary information provided by a caller along with the query. It is
    opaque to the Async RPC protocol, and may not be present on all queries. Metadata
    should generally be small, middleware-provided data that does not affect the callee's
    behavior (e.g. tracing ids). It may be subject to truncation if values provided are
    too large. See [Connection.create] for more info. *)

open! Core

module V1 : sig
  type t [@@deriving bin_io ~localize, globalize, sexp_of]

  val of_string : string -> t
  val to_string : t -> string
  val truncate : t -> int -> t
  val bin_read_t__local : t Bin_prot.Read.reader__local
end

(** Retrieves the metadata in the context of the current RPC call, if it is available. *)
val get : unit -> V1.t option

module Private : sig
  val set
    :  V1.t option
    -> Async_kernel.Execution_context.t
    -> Async_kernel.Execution_context.t
end
