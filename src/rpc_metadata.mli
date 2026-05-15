open! Core
include module type of Async_rpc_kernel_types.Rpc_metadata

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
