open Core

(** An ['a t] describes how [Async_rpc] can determine whether an rpc response of type ['a]
    should count as an error. This does not affect serialization of responses. It only
    affects the tracing information that may be listened to. *)
type _ t =
  | Only_on_exn : _ t
      (** All responses are treated as successful. If your implementation raises an uncaught
      exn first, it will still be treated as an error. *)
  | Or_error : _ Or_error.t t
      (** If the response is [Error (_ : Error.t)], it is treated as an error *)
  | Result : (_, _) Result.t t
      (** If the response is [Error _], it is treated as an error *)
  | Or_error_or_error : _ Or_error.t Or_error.t t
      (** If the response is [Error (_ : Error.t)] or [Ok (Error (_ : Error.t))], it is
      treated as an error *)
  | Result_or_error : (_, _) Result.t Or_error.t t
      (** If the response is [Error (_ : Error.t)] or [Ok (Error _)], it is treated as an
      error *)
  | Or_error_result : (_ Or_error.t, _) Result.t t
      (** If the response is [Error _] or [Ok (Error (_ : Error.t))], it is treated as an
      error *)
  | Result_result : ((_, _) Result.t, _) Result.t t
      (** If the response is [Error _] or [Ok (Error _)], it is treated as an
      error *)
  | On_none : _ Option.t t
      (** If the response is [None], it is treated as an error. For many APIs, it is
      reasonable to return no results, but you may still want to count these separately
      from nonempty responses. *)
  | Generic_or_error : 'inner t -> 'inner Or_error.t t
      (** If the response is [Error (_ : Error.t)], it is treated as an error. If it is
      [Ok x], the given rule is applied to [x] *)
  | Generic_result : 'inner t -> ('inner, _) Result.t t
      (** If the response is [Error _], it is treated as an error. If it is [Ok x], the given
      rule is applied to [x]. *)
  | Generic_option : 'inner t -> 'inner Option.t t
      (** If the response is [None], it is treated as an error. If it is [Some x], the given
      rule is applied to [x]. *)
  | Embedded : ('a -> 'b) * 'b t -> 'a t
      (** There is some value representing whether or not the rpc succeeded inside another
      object, e.g. you have a [result] inside a record with some other metadata.

      [Embedded (f, rule)] specifies a function, [f], to extract the result and a [t],
      [rule], to determine if it is an error. *)
  | Custom : { is_error : 'a -> bool } -> 'a t
      (** A function to decide if a result is an error. This is the most generic option and
      the most likely to be broken by future changes to the API. *)

module Private : sig
  val to_error_mode : 'a t -> 'a Implementation.F.error_mode
end
