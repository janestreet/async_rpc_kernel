open Core
include Protocol.Rpc_error
include Sexpable.To_stringable (Protocol.Rpc_error)

exception Rpc of t * Info.t
[@@deriving sexp ~nonportable__magic_unsafe_in_parallel_programs]

let raise t connection_description = raise (Rpc (t, connection_description))

let sexp_of_t_with_reason t ~get_connection_close_reason =
  match t with
  | Connection_closed ->
    [%sexp `Connection_closed (get_connection_close_reason () : Sexp.t)]
  | Bin_io_exn _
  | Write_error _
  | Uncaught_exn _
  | Unimplemented_rpc _
  | Unknown_query_id _
  | Authorization_failure _
  | Message_too_big _
  | Unknown _
  | Lift_error _ -> sexp_of_t t
;;

(* it would make sense to just take a [Connection.t], but we take its pieces instead to
   avoid a dependency cycle *)
let to_error
  t
  ~rpc_description:{ Description.name = rpc_name; version = rpc_version }
  ~connection_description
  ~connection_close_started
  =
  let rpc_error =
    sexp_of_t_with_reason t ~get_connection_close_reason:(fun () ->
      [%sexp (connection_close_started |> Async_kernel.Deferred.peek : Info.t option)])
  in
  Error.create_s
    [%sexp
      { rpc_error : Sexp.t
      ; connection_description : Info.t
      ; rpc_name : string
      ; rpc_version : int
      }]
;;

let implemented_in_protocol_version = function
  | Connection_closed
  | Bin_io_exn _
  | Write_error _
  | Uncaught_exn _
  | Unimplemented_rpc _
  | Unknown_query_id _ -> 1
  | Authorization_failure _ | Message_too_big _ | Unknown _ -> 3
  | Lift_error _ -> 5
;;
