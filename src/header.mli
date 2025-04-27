open! Core

type t = Protocol_version_header.t [@@deriving bin_io ~localize, globalize, sexp_of]

val bin_read_t__local : t Bin_prot.Read.reader__local
val latest : t
val v1 : t
val v2 : t
val v3 : t
val v4 : t
val v5 : t
val v6 : t
val v7 : t
val v7_only : t
val latest_at_least_v7 : t
val negotiate : us:t -> peer:t -> (int, Handshake_error.t) result
