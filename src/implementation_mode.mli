open Core
open Async_kernel

module Error_mode : sig
  type _ t =
    | Always_ok : _ t
    | Using_result : (_, _) Result.t t
    | Using_result_result : ((_, _) Result.t, _) Result.t t
    | Is_error : ('a -> bool) -> 'a t
    | Streaming_initial_message : (_, _) Protocol.Stream_initial_message.t t
end

module Result_mode : sig
  type (_, _) t =
    | Blocking : ('a, 'a Or_not_authorized.t Or_error.t) t
    | Deferred : ('a, 'a Or_not_authorized.t Or_error.t Deferred.t) t
end
