open Core_kernel.Std
open Async_kernel.Std

type 'a t = ('a, Rpc_error.t) Result.t with bin_io

type located_error =
  { location : string
  ; exn : Exn.t
  }
with sexp_of

let uncaught_exn ~location exn =
  Error (Rpc_error.Uncaught_exn (sexp_of_located_error { location; exn }))
;;

let bin_io_exn ~location exn =
  Error (Rpc_error.Bin_io_exn (sexp_of_located_error { location; exn }))
;;

let try_with ?run ~location f =
  let x = Monitor.try_with ?run f in
  let join = function
    | Ok x -> x
    | Error exn -> uncaught_exn ~location exn
  in
  match Deferred.peek x with
  | None -> x >>| join
  | Some x -> return (join x)
;;

let or_error = function
  | Ok x -> Ok x
  | Error e -> Or_error.error "rpc" e Rpc_error.sexp_of_t
;;
