@@ portable

open! Core

include module type of struct
  include Protocol.Rpc_error
end

val sexp_of_t_with_reason : t -> get_connection_close_reason:(unit -> Sexp.t) -> Sexp.t
val sexp_of_t : t -> Sexp.t

include Stringable.S with type t := t

exception Rpc of t * Info.Portable.t

val raise : t -> Info.Portable.t -> 'a
val implemented_in_protocol_version : t -> int

val to_error
  :  t
  -> rpc_description:Description.t
  -> connection_description:Info.t @ portable
  -> peek_connection_close_started_callback:(unit -> Info.Portable.t or_null @ local)
     @ local
  -> Error.t @ portable
