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
    [Single_or_streaming_error], or in rare cases, [Expert_single_succeeded_or_failed]}
    {- If a streaming RPC (e.g. a pipe or state RPC) fails, there is a single
    [Single_or_streaming_error] response}}}

    {- Received query and many streaming responses sent, all with the same description and
    id. {ol
    {- The first response would be marked as [Streaming_initial]}
    {- Followed by zero or more responses marked [Streaming_update]}
    {- A second query with the same id is a request to abort the streaming}
    {- And finished by a response marked [Streaming_closed]}}}

    {- In exceptional circumstances, e.g. if the response would be too large, there will
    be a [Failed_to_send] error and this may, for example, also lead to no
    [Streaming_closed] message ever being sent.}

    {- In exceptional cases, there will be no response. For example, if there is some kind
    of critical error that closes the connection, or if an expert unknown rpc handler
    fires.} }

    The flow for sent queries -> received responses should look like one of the following:

    {ol
    {- Sent query immediately followed by a received [One_way_so_no_response] which is
    synthetic in that no response is received over the wire.}
    {- Sent query followed by zero or more [Partial_response] events followed by one
    [Response_finished] event.}
    {- In exceptional circumstances, there will be no corresponding [Response] to a
    [Query], for example if the connection is lost or if the server never responds.}}
*)

module Sent_response_kind : sig
  type t =
    | One_way_so_no_response
        (** If the handler of a one-way RPC uses Async, this response message may be sent when
        that handler first calls [bind] rather than when it finishes its work. *)
    | Single_succeeded
    | Single_or_streaming_error
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
    | Response_finished
  [@@deriving globalize, sexp]
end

module Kind : sig
  type 'response t =
    | Query
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
