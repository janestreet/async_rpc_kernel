open! Core
include Async_rpc_kernel_types.Or_not_authorized

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
