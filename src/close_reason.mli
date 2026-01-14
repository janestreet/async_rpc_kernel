open! Core

module Closer : sig
  type t =
    | By_unknown
    | By_local
    | By_remote
  [@@deriving sexp_of]
end

module Protocol : sig
  (** The category of reasons due to which a connection was closed. *)
  module Kind : sig
    type t =
      | Other of Flexible_sexp.Variant.Stable.Other.V1.t
      | Unspecified
      | Connection_limit_reached
      | Connection_validation_failed
      (** Indicates the peer closed the connection based on their [validate_connection]
          callback in {!Connection.create}. The validation error [Error.t] provided by the
          peer can be found in [user_reason] unmodified ([Error.to_info] is the identity
          function). *)
    [@@deriving sexp, compare, variants, bin_io]
  end

  (** The reason that a connection was closed.

      [debug_info] can be arbitrarily set or appended to by the async RPC library, but it
      will never modify the [user_reason]. *)
  type t =
    { kind : Kind.t
    ; debug_info : Info_with_local_bin_io.t option
    ; user_reason : Info_with_local_bin_io.t option
    }
  [@@deriving sexp_of, compare]

  (** Create a close reason.

      [debug_info] can be arbitrarily set or appended to by the async RPC library, but it
      will never modify the [user_reason].

      [kind] defaults to {!Kind.Unspecified}. *)
  val create
    :  ?kind:Kind.t
    -> ?debug_info:Info_with_local_bin_io.t
    -> ?user_reason:Info_with_local_bin_io.t
    -> unit
    -> t

  (* For backwards compatiblity with older protocol versions. *)
  val info_of_t : t -> Info_with_local_bin_io.t

  (* We use [Flexible_sexp] so the flexible serialization is not type-equal to [t]. The
     type is exposed so that [dotnet_bin_prot] can see it and generate the correct F#
     type. *)
  module Binable : sig
    type t [@@deriving bin_io ~localize, globalize, sexp_of]
  end

  val binable_of_t : t -> Binable.t
  val t_of_binable : Binable.t -> t
end

val aux_info : Closer.t -> Protocol.t -> Info.t

type t =
  { closer : Closer.t
  ; reason : Protocol.t
  ; connection_description : Info.t
  }
[@@deriving sexp_of]

val info_of_t : t -> Info.t
