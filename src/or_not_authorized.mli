open! Core
open Async_kernel

type 'a t =
  | Authorized of 'a
  | Not_authorized of Error.t
[@@deriving sexp_of]

val lift_deferred : 'a Deferred.t t -> 'a t Deferred.t
val of_or_error : 'a Or_error.t -> 'a t
val to_or_error : 'a t -> 'a Or_error.t

(** Equivalent to [Not_authorized (Error.create_s sexp)] *)
val not_authorized_s : Sexp.t -> 'a t

(** Equivalent to [Not_authorized (Error.of_string string)] *)
val not_authorized_string : string -> 'a t

(** Collect all [Not_authorized] errors if any exist or all [Authorized] elements
    otherwise. Identical to [Or_error.combine_errors]. *)
val combine : 'a t list -> 'a list t

(** Return the first [Authorized] if one exists exist, collect all [Not_authorized] errors
    otherwise. Identical to [Or_error.find_ok]. *)
val find_authorized : 'a t list -> 'a t

(** Return [Authorized `Authorized] if all elements are [Authorized `Authorized] and
    collect all [Not_authorized] errors otherwise *)
val all_authorized : [ `Authorized ] t list -> [> `Authorized ] t

val is_authorized : 'a t -> bool

include Monad.S with type 'a t := 'a t
module Deferred : Monad.S with type 'a t := 'a t Deferred.t
