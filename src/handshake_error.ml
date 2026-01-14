open! Core

module Step = struct
  module T = struct
    type t =
      | Header
      | Connection_metadata
    [@@deriving globalize, sexp]
  end

  include T
  include Sexpable.To_stringable (T)
end

module T = struct
  type t =
    | Eof_during_step of Step.t
    | Transport_closed_during_step of Step.t
    | Transport_closed_with_reason_from_remote_during_step of
        { step : Step.t
        ; close_reason : Info.t
        }
    | Transport_pushed_back
    | Timeout
    | Reading_message_failed_during_step of
        { step : Step.t
        ; parse_error : Error.t
        }
    | Negotiation_failed of Error.t
    | Negotiated_unexpected_version of int
    | Message_too_big_during_step of
        { step : Step.t
        ; message_too_big : Transport.Send_result.message_too_big
        }
    | Unexpected_message_during_connection_metadata of Error.t
    | Connection_validation_failed of Error.t
  [@@deriving globalize, sexp]
end

include T
include Sexpable.To_stringable (T)

exception Handshake_error of (t * Info.t)
[@@deriving sexp ~nonportable__magic_unsafe_in_parallel_programs]

let to_exn ~connection_description t = Handshake_error (t, connection_description)
let to_error ~connection_description t = to_exn ~connection_description t |> Error.of_exn
