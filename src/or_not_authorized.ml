open! Core

type 'a t =
  | Authorized of 'a
  | Not_authorized of Error.t

let bind t ~f =
  match t with
  | Authorized a -> f a
  | Not_authorized _ as t -> t
;;

let map t ~f = bind t ~f:(fun x -> Authorized (f x))

let bind_deferred t ~f =
  match t with
  | Authorized a -> f a
  | Not_authorized _ as t -> Eager_deferred.return t
;;

let map_deferred t ~f =
  bind_deferred t ~f:(fun a -> Eager_deferred.map (f a) ~f:(fun a -> Authorized a))
;;

let lift_deferred = map_deferred ~f:Fn.id
