open! Core

type 'a t =
  | Authorized of 'a
  | Not_authorized of Error.t
[@@deriving sexp_of]

[%%template
[@@@mode.default p = (portable, nonportable)]

let of_or_error = function
  | Ok a -> Authorized a
  | Error e -> Not_authorized e
;;

let to_or_error = function
  | Authorized a -> Ok a
  | Not_authorized e -> Error e
;;]

let not_authorized_s sexp = Not_authorized (Error.create_s sexp)
let not_authorized_string string = Not_authorized (Error.of_string string)

include%template
  Monad.Of_monad [@mode portable]
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

let portabilize = function
  | Authorized a -> Authorized (Modes.Portable.cross a)
  | Not_authorized e -> Not_authorized (Error.portabilize e)
;;
