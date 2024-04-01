open Core

(** A bit of information about requests and responses. One can subscribe to these events
    for a {!Connection.t} to implement some kinds of metrics or tracing.

    When subscribing to events, the expected flow for received queries -> sent responses
    should look like one of the following cases.

    {ol
    {- Received query and sent response with matching rpc description and id. The response
    depends on the kind of RPC and result. {ol
    {- For one-way queries, the result is synthetic in that nothing is sent over the
    network}
    {- For an ordinary RPC, the responses may be [Single_succeeded],
    [Single_or_streaming_rpc_error_or_exn], [Single_or_streaming_user_defined_error], or
    in rare cases, [Expert_single_succeeded_or_failed]}
    {- If a streaming RPC (e.g. a pipe or state RPC) fails (e.g. an exn or authorization
    failure), there is a single [Single_or_streaming_rpc_error_or_exn] response. If it
    returns an initial error, there is a single [Single_or_streaming_user_defined_error]
    response}}}

    {- Received query and many streaming responses sent, all with the same description and
    id. {ol
    {- The first response would be marked as [Streaming_initial]}
    {- Followed by zero or more responses marked [Streaming_update]}
    {- And finished by a response marked [Streaming_closed]}
    {- An [Abort_streaming_rpc_query] with the same id is a request from the client to
    stop streaming early and may arrive at any time after the [Streaming_initial]
    message}}}

    {- In exceptional circumstances, e.g. if the response would be too large, there will
    be a [Failed_to_send] error and this may, for example, also lead to no
    [Streaming_closed] message ever being sent.}

    {- In exceptional cases, there will be no response. For example, if there is some kind
    of critical error that closes the connection, or if an expert unknown rpc handler
    fires.} }

    The difference between a [Single_or_streaming_rpc_error_or_exn] and
    [Single_or_streaming_user_defined_error] is that the former comes from errors like an
    implementation raising or returning unauthorized whereas the latter comes from an
    implementation returning an [Error _] response, for those created with
    {!Rpc.Rpc.create_result}.

    The flow for sent queries -> received responses should look like one of the following:

    {ol
    {- Sent query immediately followed by a received [One_way_so_no_response] which is
    synthetic in that no response is received over the wire.}
    {- Sent query followed by zero or more [Partial_response] events followed by one
    finished event, either [Response_finished_ok], [Response_finished_rpc_error_or_exn],
    [Response_finished_user_defined_error], or [Response_finished_expert_uninterpreted].}
    {- In exceptional circumstances, there will be no corresponding [Response] to a
    [Query], for example if the connection is lost or if the server never responds.}}
*)

module Sent_response_kind : sig
  type t =
    | One_way_so_no_response
        (** If the handler of a one-way RPC uses Async, this response message may be sent when
        that handler first calls [bind] rather than when it finishes its work. *)
    | Single_succeeded
    | Single_or_streaming_rpc_error_or_exn
    | Single_or_streaming_user_defined_error
    | Expert_single_succeeded_or_failed
        (** We can't always tell if an Expert response was successful so sometimes use this
        variant. *)
    | Streaming_initial
    | Streaming_update
    | Streaming_closed
  [@@deriving globalize, sexp]
end

module Received_response_kind : sig
  type t =
    | One_way_so_no_response
    | Partial_response
    | Response_finished_ok (** The response was interpreted as successful  *)
    | Response_finished_rpc_error_or_exn of Rpc_error.t
        (** The response was an rpc-level error, e.g. the implementation raised or its
        response was too large to send. *)
    | Response_finished_user_defined_error
        (** The response was successfully deserialized and determined to be some
        application-specific error *)
    | Response_finished_expert_uninterpreted
        (** For some expert dispatches, we can’t always tell if a response was successful so
        use this variant. *)
  [@@deriving globalize, sexp]
end

module Kind : sig
  type 'response t =
    | Query
    | Abort_streaming_rpc_query
    | Response of 'response
  [@@deriving globalize, sexp]
end

module Send_failure : sig
  type t =
    | Closed
    | Too_large
  [@@deriving globalize, sexp]
end

module Event : sig
  type t =
    | Sent of Sent_response_kind.t Kind.t
    | Failed_to_send of Sent_response_kind.t Kind.t * Send_failure.t
    | Received of Received_response_kind.t Kind.t
  [@@deriving globalize, sexp]
end

type t =
  { event : Event.t
  ; rpc : Description.t option
      (** Associated information for the RPC. This is [None] only for [Received (Response _)]
      events. *)
  ; id : Int63.t
      (** A unique identifier per in-flight RPC, this can be used to tie together request
      and response events. The client and server see the same id. *)
  ; payload_bytes : int
      (** The number of bytes for the message, except for the 8-byte length header before
      async-rpc messages. This includes the bytes specifying the rpc name/version or query
      id. This will be 0 in some cases where nothing is sent. *)
  }
[@@deriving sexp_of, globalize]
