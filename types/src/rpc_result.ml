open Core

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
