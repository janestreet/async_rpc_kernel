open! Core
include Protocol.Header

let create ~supported_versions =
  Protocol_version_header.create_exn () ~protocol:Rpc ~supported_versions
;;

(* [latest] is used as the [Connection]'s default header *)
let v7 = create ~supported_versions:[ 1; 2; 3; 4; 5; 6; 7 ]
let latest = v7
let v6 = create ~supported_versions:[ 1; 2; 3; 4; 5; 6 ]
let v5 = create ~supported_versions:[ 1; 2; 3; 4; 5 ]
let v4 = create ~supported_versions:[ 1; 2; 3; 4 ]
let v3 = create ~supported_versions:[ 1; 2; 3 ]
let v2 = create ~supported_versions:[ 1; 2 ]
let v1 = create ~supported_versions:[ 1 ]
let v7_only = create ~supported_versions:[ 7 ]
let latest_at_least_v7 = v7_only

let negotiate ~us ~peer =
  negotiate ~allow_legacy_peer:true ~us ~peer
  |> Result.map_error ~f:(fun error -> Handshake_error.Negotiation_failed error)
;;
