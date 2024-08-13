open! Core

type t =
  | Peer_metadata
  | Close_reason
[@@deriving enumerate]

let minimum_version = function
  | Peer_metadata -> 3
  | Close_reason -> 4
;;

let is_supported t ~version = version >= minimum_version t
