open! Core

type t =
  { negotiated_protocol_version : int Set_once.t
  ; writer : Transport.Writer.t
  ; tracing_events : (local_ Tracing_event.t -> unit) Bus.Read_write.t
  }
[@@deriving sexp_of]

let write_tracing_event t (local_ event) =
  if not (Bus.is_closed t.tracing_events) then Bus.write_local t.tracing_events event
;;

let sexp_of_writer t = [%sexp_of: Transport.Writer.t] t.writer

let create_before_negotiation writer ~tracing_events =
  { negotiated_protocol_version = Set_once.create (); writer; tracing_events }
;;

let set_negotiated_protocol_version t negotiated_protocol_version =
  Set_once.set_exn t.negotiated_protocol_version negotiated_protocol_version
;;

module For_handshake = struct
  let handle_handshake_result ~step : local_ _ Transport.Send_result.t -> _ = function
    | Sent { result = (); bytes = (_ : int) } -> Ok ()
    | Closed -> Error (Handshake_error.Transport_closed_during_step step)
    | Message_too_big error ->
      Error
        (Handshake_error.Message_too_big_during_step
           { message_too_big = [%globalize: Transport.Send_result.message_too_big] error
           ; step
           })
  ;;

  let send_handshake_header t header =
    (Transport.Writer.send_bin_prot t.writer Header.bin_t.writer header
     |> handle_handshake_result ~step:Handshake_error.Step.Header)
    [@nontail]
  ;;

  let send_connection_metadata_if_supported t menu ~identification =
    match Set_once.get t.negotiated_protocol_version with
    | Some version when Version_dependent_feature.is_supported Peer_metadata ~version ->
      if Version_dependent_feature.is_supported Peer_metadata_v2 ~version
      then (
        ((Transport.Writer.send_bin_prot
            t.writer
            (Protocol.Message.bin_writer_maybe_needs_length
               Protocol.Connection_metadata.V2.bin_writer_t)
            (Protocol.Message.Metadata_v2 { identification; menu })
          |> handle_handshake_result ~step:Handshake_error.Step.Connection_metadata)
         [@nontail]))
      else (
        (* We used to unconditionally send rpc shapes over the wire. It turns out that
           nobody was using them so it's fine to break backwards compatibility and send
           some garabage digest to save computation time while talking to older versions. *)
        let garbage_digest = Rpc_shapes.Just_digests.Unknown in
        let menu =
          Option.bind menu ~f:(fun menu ->
            if Menu.includes_shape_digests menu
            then Menu.Stable.V3.to_v2_response menu
            else
              Menu.supported_rpcs menu
              |> List.map ~f:(fun description -> description, garbage_digest)
              |> Some)
        in
        (Transport.Writer.send_bin_prot
           t.writer
           (Protocol.Message.bin_writer_maybe_needs_length
              Protocol.Connection_metadata.V1.bin_writer_t)
           (Protocol.Message.Metadata { identification; menu })
         |> handle_handshake_result ~step:Handshake_error.Step.Connection_metadata)
        [@nontail])
    | None
    (* [None] should be impossible to hit, but it doesn't hurt much to not send metadata
       in the case where this assumption becomes broken by other changes *)
    | Some (_ (* Version before [Metadata] *) : int) -> Ok ()
  ;;
end

let send_heartbeat t = exclave_
  Transport.Writer.send_bin_prot t.writer Protocol.Message.bin_writer_nat0_t Heartbeat
;;

let send_close_reason_if_supported t ~reason = exclave_
  match Set_once.get t.negotiated_protocol_version with
  | None -> None
  | Some version ->
    if Version_dependent_feature.is_supported Close_reason_v2 ~version
    then
      Some
        (Transport.Writer.send_bin_prot
           t.writer
           Protocol.Message.bin_writer_nat0_t
           (Close_reason_v2 (Close_reason.Protocol.binable_of_t reason)))
    else if Version_dependent_feature.is_supported Close_reason ~version
    then
      Some
        (Transport.Writer.send_bin_prot
           t.writer
           Protocol.Message.bin_writer_nat0_t
           (Close_reason (Close_reason.Protocol.info_of_t reason)))
    else None
;;

let send_close_started_if_supported t = exclave_
  match Set_once.get t.negotiated_protocol_version with
  | None -> None
  | Some version ->
    if Version_dependent_feature.is_supported Close_started ~version
    then
      Some
        (Transport.Writer.send_bin_prot
           t.writer
           Protocol.Message.bin_writer_nat0_t
           Close_started)
    else None
;;

let of_writer f t = f t.writer
let can_send = of_writer Transport.Writer.can_send
let bytes_to_write = of_writer Transport.Writer.bytes_to_write
let bytes_written = of_writer Transport.Writer.bytes_written
let flushed = of_writer Transport.Writer.flushed
let stopped = of_writer Transport.Writer.stopped
let close = of_writer Transport.Writer.close
let is_closed = of_writer Transport.Writer.is_closed

module Query = struct
  let query_message t query ~peer_menu : _ Protocol.Message.t =
    match Set_once.get_exn t.negotiated_protocol_version with
    | 1 -> Query_v1 (Protocol.Query.Validated.to_v1 query)
    | version ->
      let use_v4 = Version_dependent_feature.is_supported Query_v4 ~version in
      if use_v4
      then
        (* Check if we can optimize by sending the rank of the RPC rather than the full
           name and index *)
        Query_v4 (Protocol.Query.Validated.to_v4 query ~callee_menu:peer_menu)
      else if Version_dependent_feature.is_supported Query_metadata_v2 ~version
      then Query_v3 (Protocol.Query.Validated.to_v3 query)
      else Query_v2 (Protocol.Query.Validated.to_v2 query)
  ;;

  let send t query ~bin_writer_query ~peer_menu = exclave_
    let message = query_message t ~peer_menu query in
    Transport.Writer.send_bin_prot
      t.writer
      (Protocol.Message.bin_writer_maybe_needs_length
         (Writer_with_length.of_writer bin_writer_query))
      message
  ;;

  let send_expert t query ~buf ~pos ~len ~send_bin_prot_and_bigstring ~peer_menu
    = exclave_
    let query_with_len_data =
      { query with Protocol.Query.Validated.data = Nat0.of_int_exn len }
    in
    let header = query_message t ~peer_menu query_with_len_data in
    send_bin_prot_and_bigstring
      t.writer
      Protocol.Message.bin_writer_nat0_t
      header
      ~buf
      ~pos
      ~len
  ;;
end

module Response = struct
  let response_message (type a) t id impl_menu_index ~data : a Protocol.Message.t =
    let negotiated_protocol_version = Set_once.get_exn t.negotiated_protocol_version in
    let data =
      match data with
      | Ok (_ : a) -> data
      | Error rpc_error ->
        let error_implemented_in_protocol_version =
          Rpc_error.implemented_in_protocol_version rpc_error
        in
        (* We added [Unknown] in v3 to act as a catchall for future protocol errors.
           Before v3 we used [Uncaught_exn] as the catchall. *)
        if error_implemented_in_protocol_version <= negotiated_protocol_version
        then data
        else (
          let error_sexp = [%sexp_of: Protocol.Rpc_error.t] rpc_error in
          Error
            (if negotiated_protocol_version >= 3
             then Unknown error_sexp
             else Uncaught_exn error_sexp))
    in
    if Version_dependent_feature.is_supported
         Response_v2
         ~version:negotiated_protocol_version
    then Response_v2 { id; impl_menu_index; data }
    else Response_v1 { id; data }
  ;;

  let send t id impl_menu_index ~data ~bin_writer_response = exclave_
    let message = response_message t id impl_menu_index ~data in
    Transport.Writer.send_bin_prot
      t.writer
      (Protocol.Message.bin_writer_maybe_needs_length
         (Writer_with_length.of_writer bin_writer_response))
      message
  ;;

  let send_write_error t id sexp ~impl_menu_index =
    match
      send
        t
        id
        impl_menu_index
        ~data:(Error (Write_error sexp))
        ~bin_writer_response:Nothing.bin_writer_t
    with
    | Sent { result = (); bytes = _ } | Closed -> ()
    | Message_too_big _ as r ->
      raise_s
        [%sexp
          "Failed to send write error to client"
          , { error = (sexp : Sexp.t)
            ; reason =
                ([%globalize: unit Transport.Send_result.t] r
                 : unit Transport.Send_result.t)
            }]
  ;;

  let handle_send_result
    t
    (local_ qid)
    (local_ impl_menu_index)
    (local_ rpc)
    (local_ kind)
    (local_ (result : _ Transport.Send_result.t))
    =
    let id = (qid : Protocol.Query_id.t :> Int63.t) in
    (match result with
     | Sent { result = _; bytes } ->
       write_tracing_event
         t
         { event = Sent (Response kind); rpc; id; payload_bytes = bytes }
     | Closed ->
       write_tracing_event
         t
         { event = Failed_to_send (Response kind, Closed); rpc; id; payload_bytes = 0 }
     | Message_too_big err as r ->
       write_tracing_event
         t
         { event = Failed_to_send (Response kind, Too_large)
         ; rpc
         ; id
         ; payload_bytes = err.size
         };
       send_write_error
         t
         qid
         ([%sexp_of: unit Transport.Send_result.t]
            ([%globalize: unit Transport.Send_result.t] r))
         ~impl_menu_index);
    ()
  ;;

  let send_expert t query_id impl_menu_index ~buf ~pos ~len ~send_bin_prot_and_bigstring
    = exclave_
    let header =
      response_message t query_id impl_menu_index ~data:(Ok (Nat0.of_int_exn len))
    in
    send_bin_prot_and_bigstring
      t.writer
      Protocol.Message.bin_writer_nat0_t
      header
      ~buf
      ~pos
      ~len
  ;;
end

module Unsafe_for_cached_streaming_response_writer = struct
  let response_message = Response.response_message

  let send_bin_prot t bin_writer a = exclave_
    Transport.Writer.send_bin_prot t.writer bin_writer a
  ;;

  let send_bin_prot_and_bigstring t bin_writer a ~buf ~pos ~len = exclave_
    Transport.Writer.send_bin_prot_and_bigstring t.writer bin_writer a ~buf ~pos ~len
  ;;

  let send_bin_prot_and_bigstring_non_copying t bin_writer a ~buf ~pos ~len = exclave_
    Transport.Writer.send_bin_prot_and_bigstring_non_copying
      t.writer
      bin_writer
      a
      ~buf
      ~pos
      ~len
  ;;

  let transfer t pipe_reader f = Transport.Writer.transfer t.writer pipe_reader f
end
