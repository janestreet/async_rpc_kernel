open! Core

type t =
  | Eof
  | Transport_closed
  | Timeout
  | Reading_header_failed of Error.t
  | Negotiation_failed of Error.t
  | Negotiated_unexpected_version of int
  | Message_too_big of Transport.Send_result.message_too_big
[@@deriving sexp]

include Stringable.S with type t := t

val to_exn : connection_description:Info.t -> t -> exn
