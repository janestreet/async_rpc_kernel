open! Core

type t = Protocol_version_header.t [@@deriving bin_io, sexp_of]

val latest : t
val v1 : t
val v2 : t
val v3 : t
val v4 : t
val v5 : t
val v6 : t
val negotiate : us:t -> peer:t -> (int, Handshake_error.t) result
