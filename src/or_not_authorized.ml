open! Core

type 'a t =
  | Authorized of 'a
  | Not_authorized of Error.t
[@@deriving sexp_of]

let of_or_error = function
  | Ok a -> Authorized a
  | Error e -> Not_authorized e
;;

let to_or_error = function
  | Authorized a -> Ok a
  | Not_authorized e -> Error e
;;

let not_authorized_s sexp = Not_authorized (Error.create_s sexp)
let not_authorized_string string = Not_authorized (Error.of_string string)

include
  Monad.Of_monad
    (Or_error)
    (struct
      type nonrec 'a t = 'a t

      let to_monad = to_or_error
      let of_monad = of_or_error
    end)

let combine ts = ts |> List.map ~f:to_or_error |> Or_error.combine_errors |> of_or_error
let find_authorized ts = ts |> List.map ~f:to_or_error |> Or_error.find_ok |> of_or_error

let all_authorized ts =
  match combine ts with
  | Authorized (_ : [ `Authorized ] list) -> Authorized `Authorized
  | Not_authorized (_ : Error.t) as not_authorized -> not_authorized
;;

let is_authorized = function
  | Authorized _ -> true
  | Not_authorized (_ : Error.t) -> false
;;

module Deferred =
  Monad.Of_monad
    (Eager_deferred.Or_error)
    (struct
      type nonrec 'a t = 'a t Eager_deferred.Use.Deferred.t

      let to_monad = Eager_deferred.map ~f:to_or_error
      let of_monad = Eager_deferred.map ~f:of_or_error
    end)

let lift_deferred = function
  | Authorized a -> Eager_deferred.map a ~f:return
  | Not_authorized _ as not_authorized -> Eager_deferred.return not_authorized
;;
