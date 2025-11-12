(** Defines RPC features that are dependent on the protocol version *)
open! Core

type t =
  | Peer_metadata
  | Close_reason
  | Peer_metadata_v2
  | Response_v2
  | Query_metadata_v2
  | Close_started
  | Close_reason_v2
  | Query_v4
[@@deriving enumerate]

val is_supported : t -> version:int -> bool
