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

let minimum_version = function
  | Peer_metadata -> 3
  | Close_reason -> 4
  | Peer_metadata_v2 -> 6
  | Response_v2 -> 7
  | Query_metadata_v2 -> 8
  | Close_started -> 9
  | Close_reason_v2 -> 10
  | Query_v4 -> 11
;;

let is_supported t ~version = version >= minimum_version t
