open! Core

type 'a t =
  | Authorized of 'a
  | Not_authorized of Error.t
[@@deriving sexp_of]

[%%template:
[@@@mode.default p = (portable, nonportable)]

val of_or_error : 'a Or_error.t -> 'a t
val to_or_error : 'a t -> 'a Or_error.t]

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

(** Constructs a portable [Or_not_authorized.t] out of a possibly non-portable one, by
    calling {!Error.portabilize} on the error variant. The ['a] must cross portability.

    As with {!Error.portabilize}, this function is not a no-op in the error case; errors
    can contain lazy values, which this function must force in either to produce a
    portable error. *)
val portabilize : 'a. 'a t -> 'a t

include Monad.S with type 'a t := 'a t
