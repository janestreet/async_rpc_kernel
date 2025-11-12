open! Core
include Protocol.Header

let create ~supported_versions =
  Protocol_version_header.create_exn () ~protocol:Rpc ~supported_versions
;;

let earliest_valid_version_number = 1
let actual_latest_version_number = 11

let forced_rpc_version_number_upper_bound_env_var =
  "ASYNC_RPC_FORCE_PROTOCOL_VERSION_NUMBER_UPPER_BOUND"
;;

let latest_version_number =
  Lazy.from_fun (fun () ->
    match Sys.getenv forced_rpc_version_number_upper_bound_env_var with
    | None -> actual_latest_version_number
    | Some number ->
      (match Int.of_string_opt number with
       | None ->
         Core.print_s
           [%message
             "Invalid forced RPC version number upper bound, must be an integer version \
              number"
               ~env_var_name:(forced_rpc_version_number_upper_bound_env_var : string)
               ~value:(number : string)];
         actual_latest_version_number
       | Some number ->
         Int.clamp_exn
           ~min:earliest_valid_version_number
           ~max:actual_latest_version_number
           number))
;;

let clamp_version_number ~min version_number =
  Int.clamp_exn ~min ~max:(Lazy.force latest_version_number) version_number
;;

let versions_range start stop =
  List.range
    ~start:`inclusive
    ~stop:`inclusive
    (* NB: we don't clamp the start, as we still want to be able to enforce that we're
       talking to clients newer than a certain version, even if the override environment
       variable is set. *)
    start
    (clamp_version_number ~min:start stop)
;;

let v11 = create ~supported_versions:(versions_range 1 11)
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

(* [latest] is used as the [Connection]'s default header *)
let latest =
  create ~supported_versions:(versions_range 1 (Lazy.force latest_version_number))
;;

let v11_at_least_v7 = create ~supported_versions:(versions_range 7 11)
let v10_at_least_v7 = create ~supported_versions:(versions_range 7 10)
let v9_at_least_v7 = create ~supported_versions:(versions_range 7 9)
let v8_at_least_v7 = create ~supported_versions:(versions_range 7 8)
let v7_at_least_v7 = create ~supported_versions:(versions_range 7 7)

let latest_at_least_v7 =
  create ~supported_versions:(versions_range 7 (Lazy.force latest_version_number))
;;

let negotiate ~us ~peer =
  negotiate ~allow_legacy_peer:true ~us ~peer
  |> Result.map_error ~f:(fun error -> Handshake_error.Negotiation_failed error)
;;
