open Core
open Async_kernel

type 'a t = ('a, Rpc_error.t) Result.t [@@deriving bin_io]

let%expect_test _ =
  print_endline [%bin_digest: unit t];
  [%expect {| 106a55f7c7d8cf06dd3f4a8e759329f3 |}]
;;

type located_error =
  { location : string
  ; exn : Exn.t
  }
[@@deriving sexp_of]

let uncaught_exn ~location exn =
  Error (Rpc_error.Uncaught_exn (sexp_of_located_error { location; exn }))
;;

let bin_io_exn ~location exn =
  Error (Rpc_error.Bin_io_exn (sexp_of_located_error { location; exn }))
;;

let authorization_error ~location exn =
  Error (Rpc_error.Authorization_failure (sexp_of_located_error { location; exn }))
;;

let try_with ?on_background_exception ?run ~location f =
  let x =
    let rest =
      match on_background_exception with
      | None -> `Log
      | Some callback -> `Call callback
    in
    Monitor.try_with ~rest ~run:(Option.value run ~default:`Schedule) f
  in
  let join = function
    | Ok x -> x
    | Error exn -> uncaught_exn ~location exn
  in
  match Deferred.peek x with
  | None -> x >>| join
  | Some x -> return (join x)
;;

let or_error ~rpc_description ~connection_description ~connection_close_started =
  Result.map_error
    ~f:
      (Rpc_error.to_error
         ~rpc_description
         ~connection_description
         ~connection_close_started)
;;
