open Core

(*_ Menu lives outside of versioned RPC to avoid ciruclar dependencies *)

(** A [Menu.t] represents the RPCs implemented by a peer. In v3 of the protocol, menus are
    sent between peers as part of the handshake, and these menus also include
    {!Rpc_shapes.Just_digests.t} of the rpcs. In earlier protocol versions, the menus are
    requested by a special rpc (with name {!version_menu_rpc_name}) that is added to a set
    of implementations with {!Versioned_rpc.Menu.add}. These legacy menus do not include
    RPC shape information. *)
type t [@@deriving sexp_of]

(** The name of the rpc to request the menu  *)
val version_menu_rpc_name : string

module With_digests_in_sexp : sig
  (** Alternative sexp printer *)

  type nonrec t = t [@@deriving sexp_of]
end

(** Finds what rpcs are supported. Constructs a new list on every call *)
val supported_rpcs : t -> Description.t list

(** Finds which versions of a particular rpc are supported. Constructs a new set on every
    call *)
val supported_versions : t -> rpc_name:string -> Int.Set.t

(** Checks if a given rpc appears in the menu *)
val mem : t -> Description.t -> bool

(** Find the shape of the entry in the menu for the given rpc description. Returns None if
    and only if there is no entry. If the shape is unknown (due to the peer not supporting
    the latest rpc protocol version), [Some Unknown] is returned. *)
val shape_digests : t -> Description.t -> Rpc_shapes.Just_digests.t option

(** Similar to [supported_versions] but specific for the usecase of finding an RPC to
    execute. Unlike the roughly equivalent code,
    [Set.inter from_set (supported_versions menu ~rpc_name) |> Set.max_elt], this does
    not construct a new [Set.t] on every call. *)
val highest_available_version
  :  t
  -> rpc_name:string
  -> from_set:Int.Set.t
  -> (int, [ `Some_versions_but_none_match | `No_rpcs_with_this_name ]) Result.t

(** Helper function for both-convert rpcs. Gives nice error messages. *)
val highest_shared_version
  :  rpc_name:string
  -> callee_menu:t
  -> caller_versions:Core.Int.Set.t
  -> int Or_error.t

(** Test if there is an rpc with this name with some version in the menu *)
val has_some_versions : t -> rpc_name:string -> bool

(** Construct a menu from a list of rpcs. But note this menu won’t know about anything
    about the digests/types of the rpcs. This function exists for the legacy
    [Versioned_rpc] mechanism and shouldn’t be needed for new code. *)
val of_supported_rpcs : Description.t list -> rpc_shapes:[ `Unknown ] -> t

module Stable : sig
  module V1 : sig
    val version : int

    type query = unit [@@deriving bin_io]
    type response = (string * int) list [@@deriving bin_io, sexp_of]

    val response_of_model : Description.t list -> response
  end

  module V2 : sig
    val version : int

    type query = unit [@@deriving bin_io]

    type response = (Description.t * Rpc_shapes.Just_digests.t) list
    [@@deriving bin_io, sexp_of]
  end
end

(** Used for the {!Versioned_rpc.Menu} rpc. *)
val of_v1_response : Stable.V1.response -> t

(** Used for the v3 protocol handshake. *)
val of_v2_response : Stable.V2.response -> t
