open Core

module Sent_response_kind = struct
  type t =
    | One_way_so_no_response
    | Single_succeeded
    | Single_or_streaming_rpc_error_or_exn
    | Single_or_streaming_user_defined_error
    | Expert_single_succeeded_or_failed
    | Streaming_initial
    | Streaming_update
    | Streaming_closed
  [@@deriving globalize, sexp]
end

module Received_response_kind = struct
  type t =
    | One_way_so_no_response
    | Partial_response
    | Response_finished_ok
    | Response_finished_rpc_error_or_exn of Rpc_error.t
    | Response_finished_user_defined_error
    | Response_finished_expert_uninterpreted
  [@@deriving sexp]

  let globalize x = x
end

module Kind = struct
  type 'response t =
    | Query
    | Abort_streaming_rpc_query
    | Response of 'response
  [@@deriving globalize, sexp]
end

module Send_failure = struct
  type t =
    | Closed
    | Too_large
  [@@deriving globalize, sexp]
end

module Event = struct
  type t =
    | Sent of Sent_response_kind.t Kind.t
    | Failed_to_send of Sent_response_kind.t Kind.t * Send_failure.t
    | Received of Received_response_kind.t Kind.t
  [@@deriving globalize, sexp]
end

type t =
  { event : Event.t
  ; rpc : Description.t option
  ; id : Int63.t (* Protocol.Query_id.t is not exposed. *)
  ; payload_bytes : int
  }
[@@deriving sexp_of]

let globalize x = x
