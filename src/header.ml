open! Core
include Protocol.Header

let create ~supported_versions =
  Protocol_version_header.create_exn () ~protocol:Rpc ~supported_versions
;;

let versions_range start stop = List.range ~start:`inclusive ~stop:`inclusive start stop
let v10 = create ~supported_versions:(versions_range 1 10)
let v9 = create ~supported_versions:(versions_range 1 9)
let v8 = create ~supported_versions:(versions_range 1 8)
let v7 = create ~supported_versions:(versions_range 1 7)
let v6 = create ~supported_versions:(versions_range 1 6)
let v5 = create ~supported_versions:(versions_range 1 5)
let v4 = create ~supported_versions:(versions_range 1 4)
let v3 = create ~supported_versions:(versions_range 1 3)
let v2 = create ~supported_versions:(versions_range 1 2)
let v1 = create ~supported_versions:[ 1 ]
let latest_version_number = 10

(* [latest] is used as the [Connection]'s default header *)
let latest = create ~supported_versions:(versions_range 1 latest_version_number)
let v10_at_least_v7 = create ~supported_versions:(versions_range 7 10)
let v9_at_least_v7 = create ~supported_versions:(versions_range 7 9)
let v8_at_least_v7 = create ~supported_versions:(versions_range 7 8)
let v7_at_least_v7 = create ~supported_versions:(versions_range 7 7)

let latest_at_least_v7 =
  create ~supported_versions:(versions_range 7 latest_version_number)
;;

let negotiate ~us ~peer =
  negotiate ~allow_legacy_peer:true ~us ~peer
  |> Result.map_error ~f:(fun error -> Handshake_error.Negotiation_failed error)
;;
