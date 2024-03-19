(** Metadata is arbitrary information provided by a caller along with the query. It is
    opaque to the Async RPC protocol, and may not be present on all queries. Metadata
    should generally be small, middleware-provided data that does not affect the callee's
    behavior (e.g. tracing ids). It may be subject to truncation if values provided are
    too large. See [Connection.create] for more info. *)

open! Core

type t = string [@@deriving sexp_of]

(** Retrieves the metadata in the context of the current RPC call, if it is available. *)
val get : unit -> t option

module Private : sig
  val set
    :  t option
    -> Async_kernel.Execution_context.t
    -> Async_kernel.Execution_context.t
end
