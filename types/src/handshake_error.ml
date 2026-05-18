open! Core

module Step = struct
  module T = struct
    type t : immutable_data =
      | Header
      | Connection_metadata
    [@@deriving globalize, sexp ~portable]
  end

  include T

  [%%template include Sexpable.To_stringable [@modality portable] (T)]
end

module T = struct
  type t : immutable_data with Error.Portable.t with Info.Portable.t =
    | Eof_during_step of Step.t
    | Transport_closed_during_step of Step.t
    | Transport_closed_with_reason_from_remote_during_step of
        { step : Step.t
        ; close_reason : Info.Portable.t
        }
    | Transport_pushed_back
    | Timeout
    | Reading_message_failed_during_step of
        { step : Step.t
        ; parse_error : Error.Portable.t
        }
    | Negotiation_failed of Error.Portable.t
    | Negotiated_unexpected_version of int
    | Message_too_big_during_step of
        { step : Step.t
        ; message_too_big : Transport.Send_result.message_too_big
        }
    | Unexpected_message_during_connection_metadata of Error.Portable.t
    | Connection_validation_failed of Error.Portable.t
  [@@deriving globalize, sexp ~portable]
end

include T

[%%template include Sexpable.To_stringable [@modality portable] (T)]

exception Handshake_error of (t * Info.Portable.t) [@@deriving sexp]

let to_exn ~connection_description t = Handshake_error (t, connection_description)
let to_error ~connection_description t = to_exn ~connection_description t |> Error.of_exn
