open! Core

module T = struct
  type t =
    | Eof
    | Transport_closed
    | Transport_pushed_back
    | Timeout
    | Reading_header_failed of Error.t
    | Negotiation_failed of Error.t
    | Negotiated_unexpected_version of int
    | Message_too_big of Transport.Send_result.message_too_big
  [@@deriving globalize, sexp]
end

include T
include Sexpable.To_stringable (T)

exception Handshake_error of (t * Info.t) [@@deriving sexp]

let to_exn ~connection_description t = Handshake_error (t, connection_description)
let to_error ~connection_description t = to_exn ~connection_description t |> Error.of_exn
