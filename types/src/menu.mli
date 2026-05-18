open Core

(*_ Menu lives outside of versioned RPC to avoid ciruclar dependencies *)

(** A [Menu.t] represents the RPCs implemented by a peer. In v3 of the async-rpc protocol,
    menus are sent between peers as part of the handshake, and these menus also include
    {!Rpc_shapes.Just_digests.t} of the rpcs. *)
type t [@@deriving globalize, sexp_of]

(** Construct a menu from a list of rpcs. But note this menu won’t know about anything
    about the digests/types of the rpcs. *)
val of_supported_rpcs : Description.t list -> rpc_shapes:[ `Unknown ] -> t

(** Construct a menu from a list of rpcs and shape digests. *)
val of_supported_rpcs_and_shapes : (Description.t * Rpc_shapes.Just_digests.t) list -> t

(** The name of the rpc to request the menu *)
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

val get : t -> int -> local_ Description.t Modes.Global.t option
val index : t -> local_ Description.t -> local_ int option
val index__local : t -> tag:local_ string -> version:local_ int -> local_ int option

(** Checks if a given rpc appears in the menu *)
val mem : t -> local_ Description.t -> bool

val includes_shape_digests : t -> bool

(** Find the shape of the entry in the menu for the given rpc description. Returns None if
    and only if there is no entry. If the shape is unknown (due to the peer not supporting
    the latest rpc protocol version), [Some Unknown] is returned. *)
val shape_digests : t -> local_ Description.t -> Rpc_shapes.Just_digests.t option

(** Similar to [supported_versions] but specific for the usecase of finding an RPC to
    execute. Unlike the roughly equivalent code,
    [Set.inter from_set (supported_versions menu ~rpc_name) |> Set.max_elt], this does not
    construct a new [Set.t] on every call. *)

val highest_available_version
  :  t
  -> rpc_name:string
  -> from_sorted_array:int array
  -> local_ (int, [ `Some_versions_but_none_match | `No_rpcs_with_this_name ]) Result.t
[@@zero_alloc]

(** Helper function for both-convert rpcs. Gives nice error messages. *)
val highest_shared_version
  :  rpc_name:string
  -> callee_menu:t
  -> caller_versions:Core.Int.Set.t
  -> int Or_error.t

(** Test if there is an rpc with this name with some version in the menu *)
val has_some_versions : t -> rpc_name:string -> bool

(** [check_digests_consistent t1 t2] returns whether shape digests are consistent for all
    rpcs with matching names and versions between the two menus. There is an error if
    there is a digest mismatch, either of the two menus do not [includes_shape_digests],
    or an rpc that shows up in both menus has an [Unknown] shape digest. *)
val check_digests_consistent : t -> t -> unit Or_error.t

module Stable : sig
  module V1 : sig
    val version : int

    type query = unit [@@deriving bin_io]
    type response = (string * int) list [@@deriving bin_io, sexp_of]

    val response_of_model : Description.t list -> response
  end

  module V2 : sig
    val version : int

    type query = unit [@@deriving bin_io ~localize]

    type response = (Description.t * Rpc_shapes.Just_digests.t) list
    [@@deriving bin_io ~localize, globalize, sexp_of]

    val bin_read_response__local : response Bin_prot.Read.reader__local
  end

  module V3 : sig
    val version : int

    type query = unit [@@deriving bin_io ~localize]
    type response = t [@@deriving bin_io ~localize, globalize, sexp_of]

    val bin_read_response__local : response Bin_prot.Read.reader__local
    val to_v2_response : response -> V2.response option
  end
end

(** Used for the {!Versioned_rpc.Menu} rpc. *)
val of_v1_response : Stable.V1.response -> t

(** Used for the v3 protocol handshake. *)
val of_v2_response : Stable.V2.response -> t
