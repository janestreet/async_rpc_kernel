open Core
open Async_kernel

type 'a t = ('a, Rpc_error.t) Result.t [@@deriving bin_io, globalize, sexp_of]

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

let try_with ~here (local_ f) ~location ~on_background_exception =
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
