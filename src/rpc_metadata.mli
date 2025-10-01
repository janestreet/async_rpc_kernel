(** Metadata is arbitrary information provided by a caller along with the query. It is
    opaque to the Async RPC protocol, and may not be present on all queries. Metadata
    should generally be small, middleware-provided data that does not affect the callee's
    behavior (e.g. tracing ids). It may be subject to truncation if values provided are
    too large. See [Connection.create] for more info. *)

open! Core

module V1 : sig
  type t [@@deriving bin_io ~localize, globalize, sexp_of, equal]

  val of_string : string -> t
  val to_string : t -> string
  val truncate : t -> int -> t
  val bin_read_t__local : t Bin_prot.Read.reader__local
end

module V2 : sig
  module Key : sig
    type t [@@deriving bin_io, sexp, compare, hash, equal]

    include Comparable.S with type t := t
    include Hashable.S with type t := t

    val of_int : int -> t

    (** Key used to support the legacy query metadata interface. *)
    val default_for_legacy : t
    [@@alert
      legacy_query_metadata
        "[2025-05] This key is only used to support the legacy query metadata interface \
         and should not be used."]
  end

  module Payload : sig
    type t [@@deriving bin_io, sexp, equal]

    val to_string : t -> string

    (** [of_string_maybe_truncate s] creates a metadata payload.

        WARNING: This payload may be truncated depending on the
        [?max_metadata_size_per_key:Byte_units.t] param in [Connection.create] *)
    val of_string_maybe_truncate : string -> t
  end

  type t [@@deriving bin_io ~localize, globalize, sexp_of, equal]

  val empty : t

  (** [add t ~key ~payload] removes existing entries with the same [key] *)
  val add : t -> key:Key.t -> payload:Payload.t -> t

  val find : t -> key:Key.t -> Payload.t option
  val singleton : Key.t -> Payload.t -> t
  val singleton__local : local_ Key.t -> local_ Payload.t -> local_ t
  val to_alist : t -> (Key.t, Payload.t) List.Assoc.t
  val to_alist__local : local_ t -> local_ (Key.t, Payload.t) List.Assoc.t
  val of_alist : (Key.t, Payload.t) List.Assoc.t -> t Or_error.t
  val of_table : (Key.t, Payload.t) Hashtbl.t -> t
  val to_v1 : t -> V1.t option
  val of_v1 : V1.t -> t
  val of_v1__local : local_ V1.t -> local_ t
  val truncate_payloads : t -> int -> t
  val bin_read_t__local : t Bin_prot.Read.reader__local
end

(** Retrieves the legacy metadata in the context of the current RPC call, if it is
    available. *)
val get_from_context_for_legacy : unit -> V2.Payload.t option
[@@alert
  legacy_query_metadata
    "[2025-05] Getting query metadata from the async execution context is deprecated, \
     please use [Connection.set_metadata_hooks] instead."]

module Private : sig
  val set_context_for_legacy
    :  V2.Payload.t option
    -> Async_kernel.Execution_context.t
    -> Async_kernel.Execution_context.t
  [@@alert
    legacy_query_metadata
      "[2025-05] Setting query metadata in the async execution context is deprecated, \
       please use [Connection.set_metadata_hooks] instead."]
end
