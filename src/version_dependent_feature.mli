(** Defines RPC features that are dependent on the protocol version *)
open! Core

type t =
  | Peer_metadata
  | Close_reason
[@@deriving enumerate]

val is_supported : t -> version:int -> bool
