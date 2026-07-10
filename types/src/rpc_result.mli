@@ portable

open! Core

type 'a t = 'a Protocol.Rpc_result.t [@@deriving globalize, sexp_of]

val uncaught_exn : location:string -> exn -> 'a t
val bin_io_exn : location:string -> exn -> 'a t
val authorization_failure : location:string -> exn -> 'a t
val lift_error : location:string -> exn -> 'a t
