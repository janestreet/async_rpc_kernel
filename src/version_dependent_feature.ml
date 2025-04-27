open! Core

type t =
  | Peer_metadata
  | Close_reason
  | Peer_metadata_v2
  | Response_v2
[@@deriving enumerate]

let minimum_version = function
  | Peer_metadata -> 3
  | Close_reason -> 4
  | Peer_metadata_v2 -> 6
  | Response_v2 -> 7
;;

let is_supported t ~version = version >= minimum_version t
