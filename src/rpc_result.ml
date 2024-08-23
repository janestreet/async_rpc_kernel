open Core
open Async_kernel

type 'a t = ('a, Rpc_error.t) Result.t [@@deriving bin_io]

let%expect_test _ =
  print_endline [%bin_digest: unit t];
  [%expect {| 3c5c4bdb6a7668a7a89b965be2fac1ba |}]
;;

module Located_error = struct
  type t =
    { location : string
    ; exn : Exn.t
    }
  [@@deriving sexp_of]
end

let uncaught_exn ~location exn =
  Error (Rpc_error.Uncaught_exn ([%sexp_of: Located_error.t] { location; exn }))
;;

let bin_io_exn ~location exn =
  Error (Rpc_error.Bin_io_exn ([%sexp_of: Located_error.t] { location; exn }))
;;

let authorization_failure ~location exn =
  Error (Rpc_error.Authorization_failure ([%sexp_of: Located_error.t] { location; exn }))
;;

let lift_error ~location exn =
  Error (Rpc_error.Lift_error ([%sexp_of: Located_error.t] { location; exn }))
;;

let try_with
  ~here
  (local_ f)
  description
  ~location
  ~on_background_exception
  ~close_connection_monitor
  =
  let x =
    Monitor.try_with_local
      ~here
      ?rest:
        (On_exception.to_background_monitor_rest
           on_background_exception
           description
           ~close_connection_monitor)
      f
  in
  let join = function
    | Ok x -> x
    | Error exn -> uncaught_exn ~location exn
  in
  match Eager_deferred.peek x with
  | None -> Eager_deferred.map x ~f:join
  | Some x -> Eager_deferred.return (join x)
;;

let or_error ~rpc_description ~connection_description ~connection_close_started =
  Result.map_error
    ~f:
      (Rpc_error.to_error
         ~rpc_description
         ~connection_description
         ~connection_close_started)
;;
