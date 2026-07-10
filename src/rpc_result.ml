open Core
open Async_kernel
open! Import
include Async_rpc_kernel_types.Rpc_result

let try_with ~here (f @ local) ~location ~on_background_exception =
  let result =
    Monitor.try_with_local
      ~here
      ?rest:(on_background_exception :> [ `Call of exn -> unit | `Log | `Raise ] option)
      f
  in
  let join = function
    | Ok result -> result
    | Error exn -> uncaught_exn ~location exn
  in
  match Eager_deferred.peek result with
  | None -> Eager_deferred.map result ~f:join
  | Some result -> Eager_deferred.return (join result)
;;

let or_error ~rpc_description ~connection_description ~connection_close_started =
  Result.map_error
    ~f:
      (Rpc_error.to_error
         ~rpc_description
         ~connection_description
         ~connection_close_started)
;;
