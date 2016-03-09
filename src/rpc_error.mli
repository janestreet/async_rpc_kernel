(** Internal to [Async_rpc_kernel]. *)

open! Core_kernel.Std

include module type of struct include Protocol.Rpc_error end

include Stringable.S with type t := t

exception Rpc of t * Info.t

val raise : t -> Info.t -> 'a
