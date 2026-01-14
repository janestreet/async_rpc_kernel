open Core
open Async_kernel
open Util
module P = Protocol
module Description = Description
module On_exception = On_exception
module Implementation = Implementation
module Implementations = Implementations
module Transport = Transport
module Connection = Connection
module How_to_recognize_errors = How_to_recognize_errors

(* The Result monad is also used. *)
let ( >>=~ ) = Result.( >>= )
let ( >>|~ ) = Result.( >>| )
let and_digest shapes = shapes, Rpc_shapes.eval_to_digest shapes
let default_max_message_size = 1024 * 1024 * 1024

let message_too_big_message message_too_big ~connection =
  [%sexp
    "Message cannot be sent"
    , { reason = (Message_too_big message_too_big : Connection.Dispatch_error.t)
      ; connection : Connection.t_hum_writer
      }]
;;

let handle_dispatch_bigstring_result_exn ~connection
  : (unit, Connection.Dispatch_error.t) Result.t -> [ `Ok | `Connection_closed ]
  = function
  | Ok () -> `Ok
  | Error Closed -> `Connection_closed
  | Error (Message_too_big message_too_big) ->
    raise_s (message_too_big_message ~connection message_too_big)
;;

let handle_schedule_dispatch_bigstring_result_exn ~connection
  :  (unit Deferred.t, Connection.Dispatch_error.t) Result.t
  -> [ `Flushed of unit Deferred.t | `Connection_closed ]
  = function
  | Ok d -> `Flushed d
  | Error Closed -> `Connection_closed
  | Error (Message_too_big message_too_big) ->
    raise_s (message_too_big_message ~connection message_too_big)
;;

let rpc_result_to_or_error rpc_description conn result =
  Rpc_result.or_error
    result
    ~rpc_description
    ~connection_description:(Connection.description conn)
    ~connection_close_started:(Connection.close_reason ~on_close:`started conn)
;;

module Rpc = struct
  type ('query, 'response) t =
    { tag : P.Rpc_tag.t
    ; version : int
    ; bin_query : 'query Bin_prot.Type_class.t
    ; bin_response : 'response Bin_prot.Type_class.t
    ; query_type_id : 'query Type_equal.Id.t
    ; response_type_id : 'response Type_equal.Id.t
    ; has_errors : 'response Implementation_mode.Error_mode.t
    }

  let aux_create ~name ~version ~bin_query ~bin_response ~has_errors =
    let query_type_id =
      Type_equal.Id.create ~name:[%string "%{name}:query"] sexp_of_opaque
    in
    let response_type_id =
      Type_equal.Id.create ~name:[%string "%{name}:response"] sexp_of_opaque
    in
    { tag = P.Rpc_tag.of_string name
    ; version
    ; bin_query
    ; bin_response
    ; query_type_id
    ; response_type_id
    ; has_errors
    }
  ;;

  let[@inline] create ~name ~version ~bin_query ~bin_response ~include_in_error_count =
    (* We hope to inline the below call as it is normally trivial *)
    let has_errors =
      How_to_recognize_errors.Private.to_error_mode include_in_error_count
    in
    aux_create ~name ~version ~bin_query ~bin_response ~has_errors
  ;;

  let name t = P.Rpc_tag.to_string t.tag
  let version t = t.version
  let description t = { Description.name = name t; version = version t }
  let query_type_id t = t.query_type_id
  let response_type_id t = t.response_type_id
  let bin_query t = t.bin_query
  let bin_response t = t.bin_response

  let shapes t =
    Rpc_shapes.Rpc { query = t.bin_query.shape; response = t.bin_response.shape }
  ;;

  let shapes_and_digest t = shapes t |> and_digest

  let blocking_no_authorization f state query =
    Ok (Or_not_authorized.Authorized (f state query))
  ;;

  let deferred_no_authorization f state query =
    Eager_deferred.map (f state query) ~f:(fun a -> Ok (Or_not_authorized.Authorized a))
  ;;

  let implement ~(here : [%call_pos]) ?on_exception t f =
    { Implementation.tag = t.tag
    ; version = t.version
    ; f =
        Rpc
          ( t.bin_query.reader
          , t.bin_response.writer
          , deferred_no_authorization f
          , t.has_errors
          , Deferred
          , here )
    ; shapes = lazy (shapes_and_digest t)
    ; on_exception
    }
  ;;

  let implement_with_auth
    ~(here : [%call_pos])
    ?on_exception
    t
    (f : _ -> _ -> _ Or_not_authorized.t Deferred.t)
    =
    let f_ok state query = f state query |> Eager_deferred.map ~f:Or_error.return in
    { Implementation.tag = t.tag
    ; version = t.version
    ; f =
        Rpc (t.bin_query.reader, t.bin_response.writer, f_ok, t.has_errors, Deferred, here)
    ; shapes = lazy (shapes_and_digest t)
    ; on_exception
    }
  ;;

  let implement' ~(here : [%call_pos]) ?on_exception t f =
    { Implementation.tag = t.tag
    ; version = t.version
    ; f =
        Rpc
          ( t.bin_query.reader
          , t.bin_response.writer
          , blocking_no_authorization f
          , t.has_errors
          , Blocking
          , here )
    ; shapes = lazy (shapes_and_digest t)
    ; on_exception
    }
  ;;

  let implement_with_auth' ~(here : [%call_pos]) ?on_exception t f =
    let f_ok state query = f state query |> Or_error.return in
    { Implementation.tag = t.tag
    ; version = t.version
    ; f =
        Rpc (t.bin_query.reader, t.bin_response.writer, f_ok, t.has_errors, Blocking, here)
    ; shapes = lazy (shapes_and_digest t)
    ; on_exception
    }
  ;;

  let dispatch_with_metadata' t conn query ~metadata =
    let response_handler ivar ~data ~read_buffer ~read_buffer_pos_ref
      : Connection.Response_handler_action.t
      =
      let response =
        data
        >>=~ fun len ->
        bin_read_from_bigstring
          t.bin_response.reader
          read_buffer
          ~pos_ref:read_buffer_pos_ref
          ~len
          ~location:"client-side rpc response un-bin-io'ing"
      in
      Ivar.fill_exn ivar response;
      exclave_ Remove (Ok (Determinable (response, t.has_errors)))
    in
    let query_id = P.Query_id.create () in
    Raw_rpc.dispatch
      conn
      ~tag:t.tag
      ~version:t.version
      ~bin_writer_query:t.bin_query.writer
      ~query
      ~query_id
      ~metadata
      ~f:response_handler
  ;;

  let dispatch' = dispatch_with_metadata' ~metadata:Rpc_metadata.V2.empty

  let rpc_result_to_or_error t conn result =
    rpc_result_to_or_error (description t) conn result
  ;;

  let dispatch t conn query =
    let%map result = dispatch' t conn query in
    rpc_result_to_or_error t conn result
  ;;

  let dispatch_exn t conn query = dispatch t conn query >>| Or_error.ok_exn

  module Expert = struct
    module Responder = Implementations.Expert.Rpc_responder

    let dispatch_bin_prot_with_metadata' = dispatch_with_metadata'

    let make_dispatch
      do_dispatch
      conn
      ~rpc_tag
      ~version
      ~metadata
      buf
      ~pos
      ~len
      ~handle_response
      ~handle_error
      =
      let response_handler : Connection.Response_handler.t =
        fun ~data ~read_buffer ~read_buffer_pos_ref -> exclave_
        match data with
        | Error e ->
          handle_error
            (Error.create_s
               (Rpc_error.sexp_of_t_with_reason
                  ~get_connection_close_reason:(fun () ->
                    [%sexp
                      (Deferred.peek
                         (Connection.close_reason_structured ~on_close:`started conn)
                       : Close_reason.t option)])
                  e));
          Remove (Ok Expert_indeterminate)
        | Ok len ->
          let len = (len : Nat0.t :> int) in
          let d = handle_response read_buffer ~pos:!read_buffer_pos_ref ~len in
          read_buffer_pos_ref := !read_buffer_pos_ref + len;
          if Deferred.is_determined d
          then Remove (Ok Expert_indeterminate)
          else Expert_remove_and_wait d
      in
      do_dispatch
        conn
        ~tag:(P.Rpc_tag.of_string rpc_tag)
        ~version
        ~metadata
        buf
        ~pos
        ~len
        ~response_handler:(Some response_handler)
    ;;

    let dispatch conn ~rpc_tag ~version buf ~pos ~len ~handle_response ~handle_error =
      make_dispatch
        Connection.dispatch_bigstring
        conn
        ~rpc_tag
        ~version
        ~metadata:()
        buf
        ~pos
        ~len
        ~handle_response
        ~handle_error
      |> handle_dispatch_bigstring_result_exn ~connection:conn
    ;;

    let schedule_dispatch
      conn
      ~rpc_tag
      ~version
      buf
      ~pos
      ~len
      ~handle_response
      ~handle_error
      =
      make_dispatch
        Connection.schedule_dispatch_bigstring
        conn
        ~rpc_tag
        ~version
        ~metadata:()
        buf
        ~pos
        ~len
        ~handle_response
        ~handle_error
      |> handle_schedule_dispatch_bigstring_result_exn ~connection:conn
    ;;

    let schedule_dispatch_with_metadata
      conn
      ~rpc_tag
      ~version
      ~metadata
      buf
      ~pos
      ~len
      ~handle_response
      ~handle_error
      =
      make_dispatch
        Connection.schedule_dispatch_bigstring_with_metadata
        conn
        ~rpc_tag
        ~version
        ~metadata
        buf
        ~pos
        ~len
        ~handle_response
        ~handle_error
      |> handle_schedule_dispatch_bigstring_result_exn ~connection:conn
    ;;

    type implementation_result = Implementation.Expert.implementation_result =
      | Replied
      | Delayed_response of unit Deferred.t

    let deferred_no_authorization f state responder buf ~pos ~len =
      Eager_deferred.map (f state responder buf ~pos ~len) ~f:(fun a ->
        Ok (Or_not_authorized.Authorized a))
    ;;

    let blocking_no_authorization f state responder buf ~pos ~len =
      Ok (Or_not_authorized.Authorized (f state responder buf ~pos ~len))
    ;;

    let implement ?on_exception t f =
      { Implementation.tag = t.tag
      ; version = t.version
      ; f = Rpc_expert (deferred_no_authorization f, Deferred)
      ; shapes = lazy (shapes_and_digest t)
      ; on_exception
      }
    ;;

    let implement' ?on_exception t f =
      { Implementation.tag = t.tag
      ; version = t.version
      ; f = Rpc_expert (blocking_no_authorization f, Blocking)
      ; shapes = lazy (shapes_and_digest t)
      ; on_exception
      }
    ;;

    let implement_for_tag_and_version ?on_exception ~rpc_tag ~version f =
      { Implementation.tag = P.Rpc_tag.of_string rpc_tag
      ; version
      ; f = Rpc_expert (deferred_no_authorization f, Deferred)
      ; shapes = lazy (Unknown, Unknown)
      ; on_exception
      }
    ;;

    let implement_for_tag_and_version' ?on_exception ~rpc_tag ~version f =
      { Implementation.tag = P.Rpc_tag.of_string rpc_tag
      ; version
      ; f = Rpc_expert (blocking_no_authorization f, Blocking)
      ; shapes = lazy (Unknown, Unknown)
      ; on_exception
      }
    ;;
  end
end

module One_way = struct
  type 'msg t =
    { tag : P.Rpc_tag.t
    ; version : int
    ; bin_msg : 'msg Bin_prot.Type_class.t
    ; msg_type_id : 'msg Type_equal.Id.t
    }
  [@@deriving fields ~getters]

  let name t = P.Rpc_tag.to_string t.tag

  let create ~name ~version ~bin_msg =
    let msg_type_id = Type_equal.Id.create ~name:[%string "%{name}:msg"] sexp_of_opaque in
    { tag = P.Rpc_tag.of_string name; version; bin_msg; msg_type_id }
  ;;

  let shapes t = Rpc_shapes.One_way { msg = t.bin_msg.shape }
  let shapes_and_digest t = shapes t |> and_digest
  let description t = { Description.name = name t; version = version t }
  let msg_type_id t = t.msg_type_id

  let no_authorization f state query =
    Eager_deferred.return (Ok (Or_not_authorized.Authorized (f state query)))
  ;;

  let implement ~(here : [%call_pos]) ?on_exception t f =
    { Implementation.tag = t.tag
    ; version = t.version
    ; f = One_way (t.bin_msg.reader, no_authorization f, here)
    ; shapes = lazy (shapes_and_digest t)
    ; on_exception
    }
  ;;

  let dispatch_with_metadata' t conn query ~metadata =
    let query_id = P.Query_id.create () in
    Raw_rpc.dispatch'
      conn
      ~tag:t.tag
      ~version:t.version
      ~bin_writer_query:t.bin_msg.writer
      ~query
      ~query_id
      ~metadata
      ~response_handler:None
  ;;

  let dispatch' = dispatch_with_metadata' ~metadata:Rpc_metadata.V2.empty

  let rpc_result_to_or_error t conn result =
    rpc_result_to_or_error (description t) conn result
  ;;

  let dispatch t conn query =
    dispatch' t conn query |> fun result -> rpc_result_to_or_error t conn result
  ;;

  let dispatch_exn t conn query = Or_error.ok_exn (dispatch t conn query)

  module Expert = struct
    let no_authorization f state buf ~pos ~len =
      Eager_deferred.return (Ok (Or_not_authorized.Authorized (f state buf ~pos ~len)))
    ;;

    let implement ?on_exception t f =
      { Implementation.tag = t.tag
      ; version = t.version
      ; f = One_way_expert (no_authorization f)
      ; shapes = lazy (shapes_and_digest t)
      ; on_exception
      }
    ;;

    let dispatch_bin_prot_with_metadata' = dispatch_with_metadata'

    let dispatch { tag; version; bin_msg = _; msg_type_id = _ } conn buf ~pos ~len =
      Connection.dispatch_bigstring
        conn
        ~tag
        ~version
        ~metadata:()
        buf
        ~pos
        ~len
        ~response_handler:None
      |> handle_dispatch_bigstring_result_exn ~connection:conn
    ;;

    let schedule_dispatch
      { tag; version; bin_msg = _; msg_type_id = _ }
      conn
      buf
      ~pos
      ~len
      =
      Connection.schedule_dispatch_bigstring
        conn
        ~tag
        ~version
        ~metadata:()
        buf
        ~pos
        ~len
        ~response_handler:None
      |> handle_schedule_dispatch_bigstring_result_exn ~connection:conn
    ;;
  end
end

module Pipe_close_reason = struct
  type t =
    | Closed_locally
    | Closed_remotely
    | Error of Error.t
  [@@deriving bin_io, compare ~localize, sexp]

  let%expect_test _ =
    print_endline [%bin_digest: t];
    [%expect {| 748c8bf4502d0978d007bf7f96a7ef7f |}]
  ;;

  module Stable = struct
    module V1 = struct
      type nonrec t = t =
        | Closed_locally
        | Closed_remotely
        | Error of Error.Stable.V2.t
      [@@deriving bin_io, compare ~localize, sexp]

      let%expect_test _ =
        print_endline [%bin_digest: t];
        [%expect {| 748c8bf4502d0978d007bf7f96a7ef7f |}]
      ;;
    end
  end
end

(* the basis of the implementations of Pipe_rpc and State_rpc *)
module Streaming_rpc = struct
  module Initial_message = P.Stream_initial_message

  type ('query, 'initial_response, 'update_response, 'error_response) t =
    { tag : P.Rpc_tag.t
    ; version : int
    ; bin_query : 'query Bin_prot.Type_class.t
    ; bin_initial_response : 'initial_response Bin_prot.Type_class.t
    ; bin_update_response : 'update_response Bin_prot.Type_class.t
    ; bin_error_response : 'error_response Bin_prot.Type_class.t
    ; client_pushes_back : bool
    ; query_type_id : 'query Type_equal.Id.t
    ; initial_response_type_id : 'initial_response Type_equal.Id.t
    ; update_response_type_id : 'update_response Type_equal.Id.t
    ; error_response_type_id : 'error_response Type_equal.Id.t
    }

  let create
    ?client_pushes_back
    ~name
    ~version
    ~bin_query
    ~bin_initial_response
    ~bin_update_response
    ~bin_error
    ~alias_for_initial_response
    ~alias_for_update_response
    ()
    =
    let client_pushes_back =
      match client_pushes_back with
      | None -> false
      | Some () -> true
    in
    let query_type_id =
      Type_equal.Id.create ~name:[%string "%{name}:query"] sexp_of_opaque
    in
    let initial_response_type_id =
      Type_equal.Id.create
        ~name:[%string "%{name}:%{alias_for_initial_response}"]
        sexp_of_opaque
    in
    let update_response_type_id =
      Type_equal.Id.create
        ~name:[%string "%{name}:%{alias_for_update_response}"]
        sexp_of_opaque
    in
    let error_response_type_id =
      Type_equal.Id.create ~name:[%string "%{name}:error"] sexp_of_opaque
    in
    { tag = P.Rpc_tag.of_string name
    ; version
    ; bin_query
    ; bin_initial_response
    ; bin_update_response
    ; bin_error_response = bin_error
    ; client_pushes_back
    ; query_type_id
    ; initial_response_type_id
    ; update_response_type_id
    ; error_response_type_id
    }
  ;;

  let name t = P.Rpc_tag.to_string t.tag
  let version t = t.version
  let description t = { Description.name = name t; version = version t }

  let initial_message initial =
    { Initial_message.unused_query_id = P.Unused_query_id.singleton; initial }
  ;;

  let shapes t =
    Rpc_shapes.Streaming_rpc
      { query = t.bin_query.shape
      ; initial_response = t.bin_initial_response.shape
      ; update_response = t.bin_update_response.shape
      ; error = t.bin_error_response.shape
      }
  ;;

  let shapes_and_digest t = shapes t |> and_digest

  let implement_gen
    ~(here : [%call_pos])
    ?(leave_open_on_exception = false)
    ?on_exception
    t
    impl
    =
    let bin_init_writer =
      Initial_message.bin_writer_t
        t.bin_initial_response.writer
        t.bin_error_response.writer
    in
    { Implementation.tag = t.tag
    ; version = t.version
    ; f =
        Streaming_rpc
          { bin_query_reader = t.bin_query.reader
          ; bin_init_writer
          ; bin_update_writer = t.bin_update_response.writer
          ; impl
          ; error_mode = Streaming_initial_message
          ; leave_open_on_exception
          ; here
          }
    ; shapes = lazy (shapes_and_digest t)
    ; on_exception
    }
  ;;

  let implement ~(here : [%call_pos]) ?on_exception ?leave_open_on_exception t f =
    let f c query =
      match%map f c query with
      | Error err ->
        Ok (Or_not_authorized.Authorized (Error (initial_message (Error err))))
      | Ok (initial, pipe) -> Ok (Authorized (Ok (initial_message (Ok initial), pipe)))
    in
    implement_gen ~here t ?on_exception ?leave_open_on_exception (Pipe f)
  ;;

  let implement_with_auth ~(here : [%call_pos]) ?on_exception ?leave_open_on_exception t f
    =
    let f c query =
      let%map response_or_not_authorized = f c query in
      Result.map response_or_not_authorized ~f:(fun response ->
        Or_not_authorized.map response ~f:(function
          | Error err -> Error (initial_message (Error err))
          | Ok (initial, pipe) -> Ok (initial_message (Ok initial), pipe)))
    in
    implement_gen ~here t ?on_exception ?leave_open_on_exception (Pipe f)
  ;;

  let implement_direct ~(here : [%call_pos]) ?on_exception ?leave_open_on_exception t f =
    let f c query writer =
      match%map f c query writer with
      | Error _ as x -> Ok (Or_not_authorized.Authorized (Error (initial_message x)))
      | Ok _ as x -> Ok (Authorized (Ok (initial_message x)))
    in
    implement_gen ~here ?on_exception ?leave_open_on_exception t (Direct f)
  ;;

  let implement_direct_with_auth
    ~(here : [%call_pos])
    ?on_exception
    ?leave_open_on_exception
    t
    f
    =
    let f c query writer =
      let%map response_or_not_authorized = f c query writer in
      Result.map response_or_not_authorized ~f:(fun response ->
        Or_not_authorized.map response ~f:(function
          | Error _ as x -> Error (initial_message x)
          | Ok _ as x -> Ok (initial_message x)))
    in
    implement_gen ~here ?on_exception ?leave_open_on_exception t (Direct f)
  ;;

  let abort t conn id =
    let query =
      { P.Query.Validated.tag = t.tag
      ; version = t.version
      ; id
      ; metadata = None
      ; data = `Abort
      }
    in
    ignore
      (Connection.dispatch
         conn
         ~kind:Abort_streaming_rpc_query
         ~bin_writer_query:P.Stream_query.bin_writer_nat0_t
         ~query
         ~response_handler:None
       : (unit, Connection.Dispatch_error.t) Result.t)
  ;;

  module Closed_by = struct
    type t =
      [ `By_remote_side
      | `Error of Error.t
      ]
  end

  module Pipe_message = struct
    type 'a t =
      | Update of 'a
      | Closed of Closed_by.t
  end

  module Pipe_response = struct
    type t =
      | Continue
      | Wait of unit Deferred.t
  end

  module Pipe_metadata = struct
    type t =
      { query_id : P.Query_id.t
      ; close_reason : Pipe_close_reason.t Deferred.t
      }

    let id t = t.query_id
    let close_reason t = t.close_reason
  end

  module Response_state = struct
    module Update_handler = struct
      type 'a t =
        | Standard of ('a Pipe_message.t -> Pipe_response.t)
        | Expert of
            { on_update : Bigstring.t -> pos:int -> len:int -> Pipe_response.t
            ; on_closed : Closed_by.t -> unit
            }

      let handle_closed (t : _ t) closed_by =
        match t with
        | Standard update_handler ->
          ignore (update_handler (Closed closed_by) : Pipe_response.t)
        | Expert { on_update = _; on_closed } -> on_closed closed_by
      ;;
    end

    module State = struct
      module Init_result = struct
        type ('init_response, 'error) t =
          (P.Query_id.t * 'init_response, 'error) Result.t Rpc_result.t
      end

      type ('init, 'init_response, 'update, 'error) t =
        | Waiting_for_initial_response of
            { initial_handler : 'init -> 'init_response * 'update Update_handler.t
            ; response_ivar : ('init_response, 'error) Init_result.t Ivar.t
            }
        | Waiting_for_updates of 'update Update_handler.t
        | Waiting_for_induced_close of
            { original_result : ('init_response, 'error) Init_result.t }
    end

    type nonrec ('query, 'init, 'update, 'error, 'init_response) t =
      { mutable state : ('init, 'init_response, 'update, 'error) State.t
      ; rpc : ('query, 'init, 'update, 'error) t
      ; query_id : P.Query_id.t
      ; connection : Connection.t
      ; get_connection_close_reason : unit -> Sexp.t
      }

    let we_know_that_the_server_remains_open_on_error = function
      | Rpc_error.Bin_io_exn (_ : Sexp.t)
      | Connection_closed
      | Uncaught_exn (_ : Sexp.t)
      | Unimplemented_rpc _
      | Unknown_query_id (_ : P.Query_id.t)
      | Authorization_failure (_ : Sexp.t)
      | Message_too_big (_ : Transport_intf.Send_result.message_too_big)
      | Unknown (_ : Sexp.t)
      | Lift_error (_ : Sexp.t) -> false
      | Write_error (_ : Sexp.t) -> true
    ;;

    (* Error handling is a bit complicated here so I'll attempt to explain it in words
       instead.

       [Connection.Response_handler_action.Remove] will remove the [Query_id.t] from the
       client connection loop. If a subsequent message is sent with that [Query_id.t] from
       the server writer the client's connection will throw and close the shared stream.

       This is a problem because there are cases where the server could send subsequent
       messages to a client in good faith after an error. e.g. version mismatch and the
       client can't deserialize the messages properly.

       So we want to be careful about exactly when we remove a [Query_id.t] to try and
       prevent the entire connection from closing.

       The safest way to do this is to send an [abort] to the server to induce a close
       then wait until we receive an [Eof] (which we can then [Remove] using the
       [original_result] that caused us to send the abort for proper tracing).

       In some cases however, the server will have already closed. In those cases, we
       can't send an abort and wait for [`Eof] because once the server closes the pipe, it
       will no longer respond to abort messages with [`Eof]. We need to immediately remove
       the handler instead of sending abort in that case, to avoid leaking a response
       handler.

       Differentiating these cases is done using the above
       [we_know_that_the_server_remains_open_on_error], which is based on
       [Implementations.apply_streaming_implementation:run_impl].

       Simply adjusting the server to always respond to unknown aborts with [`Eof] doesn't
       work, because then the client could in some cases receive multiple [`Eof]'s for the
       same pipe, so instead we model here whether the server has already closed the pipe.

       An improvement here would be to modify the protocol so that there's an explicit
       field or message indicating that the server has closed the pipe, and then we could
       always send [abort] unless we've already received that message.
    *)
    let response_handler t : Connection.Response_handler.t =
      fun ~data ~read_buffer ~read_buffer_pos_ref -> exclave_
      let bin_read_from_bigstring ?add_len reader ~len ~location =
        bin_read_from_bigstring
          ?add_len
          reader
          read_buffer
          ~pos_ref:read_buffer_pos_ref
          ~len
          ~location
      in
      let read_stream_response_header ~len =
        bin_read_from_bigstring
          P.Stream_response_data.bin_reader_nat0_t
          ~len
          ~location:"client-side streaming_rpc response un-bin-io'ing"
          ~add_len:(function
          | `Eof -> 0
          | `Ok (len : Nat0.t) -> (len :> int))
      in
      let on_error result ~we_know_the_server_pipe_is_open
        : Connection.Response_handler_action.t
        =
        t.state <- Waiting_for_induced_close { original_result = result };
        if we_know_the_server_pipe_is_open
        then (
          abort t.rpc t.connection t.query_id;
          Keep)
        else Remove (Ok (Determinable (result, Using_result)))
      in
      match t.state with
      | Waiting_for_updates handler ->
        let on_error error ~we_know_the_server_pipe_is_open =
          let core_error =
            Error.create_s
              (Rpc_error.sexp_of_t_with_reason
                 ~get_connection_close_reason:t.get_connection_close_reason
                 error)
          in
          (* It's always fine to close our own local reader when we want to. We just can't
             always close the connection handler for this [Query_id.t] since more messages
             might be on the way. *)
          Update_handler.handle_closed handler (`Error core_error);
          on_error (Error error) ~we_know_the_server_pipe_is_open
        in
        (match data with
         | Error error ->
           (* We know that the server will have closed it's pipe after these errors so
              let's not bother with waiting for an ack on [abort]. *)
           on_error
             error
             ~we_know_the_server_pipe_is_open:
               (we_know_that_the_server_remains_open_on_error error)
         | Ok len ->
           (match read_stream_response_header ~len with
            | Error error -> on_error error ~we_know_the_server_pipe_is_open:true
            | Ok `Eof ->
              (* Always safe to close on an [Eof]. *)
              Update_handler.handle_closed handler `By_remote_side;
              Remove (Ok Pipe_eof)
            | Ok (`Ok len) ->
              (match handler with
               | Standard update_handler ->
                 let data =
                   bin_read_from_bigstring
                     t.rpc.bin_update_response.reader
                     ~len
                     ~location:
                       "client-side streaming_rpc response un-bin-io'ing to \
                        [bin_reader_update]"
                 in
                 (match data with
                  | Error error ->
                    (* Similarly to the above case, this always implies a [Bin_io_exn]
                       when we fail to deseralize ['update]. Server won't know that we
                       failed to deserialize so let's wait for their ack to close. *)
                    on_error error ~we_know_the_server_pipe_is_open:true
                  | Ok data ->
                    (match update_handler (Update data) with
                     | Continue -> Keep
                     | Wait d -> Wait d))
               | Expert { on_update; on_closed = _ } ->
                 let pos = !read_buffer_pos_ref in
                 let len = (len :> int) in
                 read_buffer_pos_ref := pos + len;
                 (try
                    match on_update read_buffer ~pos ~len with
                    | Continue -> Keep
                    | Wait d -> Wait d
                  with
                  | exn ->
                    let error =
                      Rpc_error.Uncaught_exn
                        [%message
                          "Uncaught exception in expert update handler" (exn : Exn.t)]
                    in
                    on_error error ~we_know_the_server_pipe_is_open:false))))
      | Waiting_for_induced_close { original_result } ->
        (* We are waiting for an ack on close from the peer but have already closed our
           side. When transitioning to this state there might be other messages already on
           the pipe so we wait for a [Pipe_eof] triggered from our abort message but might
           run into another message which is fine. *)
        (match data with
         | Error error ->
           if we_know_that_the_server_remains_open_on_error error
           then Keep
           else Remove (Ok (Determinable (Error error, Using_result)))
         | Ok len ->
           (match read_stream_response_header ~len with
            | Ok `Eof -> Remove (Ok (Determinable (original_result, Using_result)))
            | Error (_ : Rpc_error.t) | Ok (`Ok (_ : Nat0.t)) ->
              (* We've marked the pipe closed on the reader end but the writer might still
                 be sending data after. Let's drop this while waiting for the error ack
                 since the handler is already cleaned up. *)
              Keep))
      | Waiting_for_initial_response { initial_handler; response_ivar } ->
        let on_error result ~we_know_the_server_pipe_is_open =
          Ivar.fill_exn response_ivar result;
          on_error result ~we_know_the_server_pipe_is_open
        in
        (* Error cases in [Waiting_for_initial_response] match [Waiting_for_updates]
           except we need to fill the [response_ivar] so that our dispatch determines. *)
        (match data with
         | Error error ->
           on_error
             (Error error)
             ~we_know_the_server_pipe_is_open:
               (we_know_that_the_server_remains_open_on_error error)
         | Ok len ->
           let initial =
             bin_read_from_bigstring
               (Initial_message.bin_reader_t
                  t.rpc.bin_initial_response.reader
                  t.rpc.bin_error_response.reader)
               ~len
               ~location:"client-side streaming_rpc initial_response un-bin-io'ing"
           in
           (match initial with
            | Error _ as result ->
              (* [Bin_io_exn] on failure to deserialize the initial response. Let's wait
                 for the peer to [Eof] since they likely don't know that an error occured
                 on our end. *)
              on_error result ~we_know_the_server_pipe_is_open:true
            | Ok initial_msg ->
              (match initial_msg.initial with
               | Error _ as result ->
                 let (_ : Connection.Response_handler_action.t) =
                   on_error (Ok result) ~we_know_the_server_pipe_is_open:false
                 in
                 (* If the peer sent an [Error] as the initial message that serialized
                    properly then they will have already closed the pipe on their end so
                    we can immediately exit. *)
                 Remove (Ok (Determinable (initial, Streaming_initial_message)))
               | Ok initial ->
                 let initial_response, handler = initial_handler initial in
                 Ivar.fill_exn response_ivar (Ok (Ok (t.query_id, initial_response)));
                 t.state <- Waiting_for_updates handler;
                 Keep)))
    ;;
  end

  let dispatch_gen t connection query ~metadata ~initial_handler =
    let bin_writer_query =
      P.Stream_query.bin_writer_needs_length
        (Writer_with_length.of_type_class t.bin_query)
    in
    let query = `Query query in
    let query_id = P.Query_id.create () in
    Raw_rpc.dispatch
      connection
      ~query_id
      ~tag:t.tag
      ~version:t.version
      ~bin_writer_query
      ~query
      ~metadata
      ~f:(fun response_ivar ->
        Response_state.response_handler
          { state = Waiting_for_initial_response { initial_handler; response_ivar }
          ; rpc = t
          ; query_id
          ; connection
          ; get_connection_close_reason =
              (fun () ->
                [%sexp
                  (Deferred.peek
                     (Connection.close_reason_structured ~on_close:`started connection)
                   : Close_reason.t option)])
          })
  ;;

  let dispatch_fold t conn query ~init ~f ~closed ~metadata =
    let result = Ivar.create () in
    match%map
      dispatch_gen t conn query ~metadata ~initial_handler:(fun state ->
        let acc = ref (init state) in
        ( ()
        , Standard
            (function
              | Update update ->
                let new_acc, response = f !acc update in
                acc := new_acc;
                response
              | Closed reason ->
                Ivar.fill_exn result (closed !acc reason);
                Continue) ))
    with
    | (Error _ | Ok (Error _)) as e -> rpc_result_to_or_error (description t) conn e
    | Ok (Ok (id, ())) -> Ok (Ok (id, Ivar.read result))
  ;;

  let dispatch' t conn query ~metadata =
    match%map
      dispatch_gen t conn query ~metadata ~initial_handler:(fun init ->
        let pipe_r, pipe_w = Pipe.create () in
        (* Set a small buffer to reduce the number of pushback events *)
        Pipe.set_size_budget pipe_w 100;
        let close_reason : Pipe_close_reason.t Ivar.t = Ivar.create () in
        (* The regular TCP transport [lib/async_rpc/core/src/rpc_transport.ml], unlike
           other transports (low latency, pipe), does not immediately bind on the
           [flushed] deferred when it receives [Wait] and instead will call [f] in a
           synchronous loop until the end of its batch of messages and [bind] on all the
           [Wait]s it received in that batch.

           Instead of allocating a bunch of deferreds in this case, we cache the deferred
           of the first [Wait] we send back. This does mean that the transport could send
           its next batch of messages before the previous batch has flushed, so we'll
           buffer at most two batches of messages on the pipe (compared to just one batch
           if we didn't cache). This is likely fine for memory usage since the transport
           already had these messages buffered anyways. *)
        let cached_flush_deferred : unit Deferred.t or_null ref = ref Null in
        let get_flush_deferred () =
          match !cached_flush_deferred with
          | This d when not (Deferred.is_determined d) -> d
          | _ ->
            incr Util.For_testing.number_of_unique_flushed_waits_due_to_streaming_pushback;
            let d =
              match%map Pipe.downstream_flushed pipe_w with
              | `Ok | `Reader_closed -> ()
            in
            cached_flush_deferred := This d;
            d
        in
        let f : _ Pipe_message.t -> Pipe_response.t = function
          | Update data ->
            if not (Pipe.is_closed pipe_w)
            then (
              Pipe.write_without_pushback pipe_w data;
              if t.client_pushes_back && Pipe.length pipe_w >= Pipe.size_budget pipe_w
              then Wait (get_flush_deferred ())
              else Continue)
            else Continue
          | Closed reason ->
            Ivar.fill_if_empty
              close_reason
              (match reason with
               | `By_remote_side -> Closed_remotely
               | `Error err -> Error err);
            Pipe.close pipe_w;
            Continue
        in
        (init, pipe_r, close_reason), Standard f)
    with
    | (Error _ | Ok (Error _)) as e -> e
    | Ok (Ok (id, (init, pipe_r, close_reason))) ->
      upon (Pipe.closed pipe_r) (fun () ->
        if not (Ivar.is_full close_reason)
        then (
          abort t conn id;
          Ivar.fill_if_empty close_reason Closed_locally));
      let pipe_metadata : Pipe_metadata.t =
        { query_id = id; close_reason = Ivar.read close_reason }
      in
      Ok (Ok (pipe_metadata, init, pipe_r))
  ;;

  let dispatch t conn query ~metadata =
    dispatch' t conn query ~metadata >>| rpc_result_to_or_error (description t) conn
  ;;
end

module Pipe_message = Streaming_rpc.Pipe_message
module Pipe_response = Streaming_rpc.Pipe_response

(* A Pipe_rpc is like a Streaming_rpc, except we don't care about initial state - thus it
   is restricted to unit and ultimately ignored *)
module Pipe_rpc = struct
  type ('query, 'response, 'error) t = ('query, unit, 'response, 'error) Streaming_rpc.t

  module Id = P.Query_id
  module Metadata = Streaming_rpc.Pipe_metadata

  let create ?client_pushes_back ~name ~version ~bin_query ~bin_response ~bin_error () =
    Streaming_rpc.create
      ?client_pushes_back
      ~name
      ~version
      ~bin_query
      ~bin_initial_response:Unit.bin_t
      ~bin_update_response:bin_response
      ~bin_error
        (* [initial_response] doesn't show up in [Pipe_rpc]'s signature, so the type-id
           created using [alias_for_initial_response] is unreachable. *)
      ~alias_for_initial_response:""
      ~alias_for_update_response:"response"
      ()
  ;;

  let bin_query t = t.Streaming_rpc.bin_query
  let bin_response t = t.Streaming_rpc.bin_update_response
  let bin_error t = t.Streaming_rpc.bin_error_response
  let shapes t = Streaming_rpc.shapes t
  let client_pushes_back t = t.Streaming_rpc.client_pushes_back

  let implement ~(here : [%call_pos]) ?on_exception ?leave_open_on_exception t f =
    Streaming_rpc.implement ~here ?on_exception ?leave_open_on_exception t (fun a query ->
      let%map x = f a query in
      x >>|~ fun x -> (), x)
  ;;

  let implement_with_auth ~(here : [%call_pos]) ?on_exception ?leave_open_on_exception t f
    =
    Streaming_rpc.implement_with_auth
      ~here
      ?on_exception
      ?leave_open_on_exception
      t
      (fun a query ->
         match%map f a query with
         | Or_not_authorized.Not_authorized _ as r -> Ok r
         | Authorized (Error _) as err -> Ok err
         | Authorized (Ok pipe) -> Ok (Authorized (Ok ((), pipe))))
  ;;

  module Direct_stream_writer = struct
    include Implementations.Direct_stream_writer

    (* Hiding the [result] optional parameter for now. See the someday in the interface
       for more information *)
    let close t = close t

    module Group = struct
      module Buffer = struct
        type t = Bigstring.t ref

        let create ?(initial_size = 4096) () =
          if initial_size < 0
          then
            failwiths
              "Rpc.Pipe_rpc.Direct_stream_writer.Group.Buffer.create got negative buffer \
               size"
              initial_size
              Int.sexp_of_t;
          ref (Bigstring.create initial_size)
        ;;
      end

      type 'a direct_stream_writer = 'a t

      module T = Implementation_types.Direct_stream_writer

      type 'a t = 'a T.Group.t =
        { mutable components : 'a direct_stream_writer Bag.t
        ; components_by_id : 'a component Id.Table.t
        ; buffer : Bigstring.t ref
        ; mutable last_value_len : int
        ; last_value_not_written : 'a Moption.t
        ; send_last_value_on_add : bool
        }

      and 'a component = 'a T.Group.component =
        { writer_element_in_group : 'a direct_stream_writer Bag.Elt.t
        ; group_element_in_writer : 'a T.group_entry Bag.Elt.t
        }

      let aux_create ?buffer () ~send_last_value_on_add =
        let buffer =
          match buffer with
          | None -> Buffer.create ()
          | Some b -> b
        in
        { components = Bag.create ()
        ; components_by_id = Id.Table.create ()
        ; buffer
        ; last_value_len = -1
        ; last_value_not_written = Moption.create ()
        ; send_last_value_on_add
        }
      ;;

      let create ?buffer () = aux_create ?buffer ~send_last_value_on_add:false ()

      let create_sending_last_value_on_add ?initial_buffer_size () =
        let buffer =
          Option.map initial_buffer_size ~f:(fun initial_size ->
            Buffer.create ~initial_size ())
        in
        aux_create ?buffer ~send_last_value_on_add:true ()
      ;;

      let length t = Bag.length t.components

      let remove t (writer : _ Implementations.Direct_stream_writer.t) =
        match Hashtbl.find_and_remove t.components_by_id writer.id with
        | None -> ()
        | Some { writer_element_in_group; group_element_in_writer } ->
          Bag.remove t.components writer_element_in_group;
          Bag.remove writer.groups group_element_in_writer
      ;;

      let to_list t = Bag.to_list t.components

      let flushed_or_closed t =
        to_list t
        |> List.map ~f:(fun t -> Deferred.any_unit [ flushed t; closed t ])
        |> Deferred.all_unit
      ;;

      let flushed t = flushed_or_closed t
      let direct_write_without_pushback = Expert.write_without_pushback

      module Expert = struct
        let write_without_pushback t ~buf ~pos ~len =
          Bag.iter t.components ~f:(fun direct_stream_writer ->
            (* Writers are automatically scheduled to be removed from their groups when
               closed, so [`Closed] here just means that the removal didn't happen yet. *)
            ignore
              (Expert.write_without_pushback direct_stream_writer ~buf ~pos ~len
               : [ `Ok | `Closed ]));
          if t.send_last_value_on_add && not (phys_equal !(t.buffer) buf)
          then (
            if Bigstring.length !(t.buffer) < len
            then t.buffer := Bigstring.create (Int.ceil_pow2 len);
            Bigstring.blit ~src:buf ~src_pos:pos ~dst:!(t.buffer) ~dst_pos:0 ~len;
            t.last_value_len <- len)
        ;;

        let write t ~buf ~pos ~len =
          write_without_pushback t ~buf ~pos ~len;
          flushed_or_closed t
        ;;
      end

      let write_without_pushback t x =
        match Bag.choose t.components with
        | None ->
          if t.send_last_value_on_add
          then (
            Moption.set_some t.last_value_not_written x;
            (* Updating [last_value_len] isn't strictly necessary, but it makes it true
               that we always either have a [last_value_not_written] OR
               [last_value_len >= 0], not both (or that we've never had a value written to
               this group). *)
            t.last_value_len <- -1)
        | Some one ->
          let one = Bag.Elt.value one in
          let { Bin_prot.Type_class.write; size } = bin_writer one in
          let buffer = !(t.buffer) in
          (* Optimistic first try *)
          (match write buffer ~pos:0 x with
           | len ->
             Expert.write_without_pushback t ~buf:buffer ~pos:0 ~len;
             t.last_value_len <- len
           | exception _ ->
             (* It's likely that the exception is due to a buffer overflow, so resize the
                internal buffer and try again. Technically we could match on
                [Bin_prot.Common.Buffer_short] only, however we can't easily enforce that
                custom bin_write_xxx functions raise this particular exception and not
                [Invalid_argument] or [Failure] for instance. *)
             let len = size x in
             Bigstring.unsafe_destroy buffer;
             let buffer = Bigstring.create (Int.ceil_pow2 len) in
             t.buffer := buffer;
             let len = write buffer ~pos:0 x in
             Expert.write_without_pushback t ~buf:buffer ~pos:0 ~len;
             t.last_value_len <- len)
      ;;

      let add_exn t (writer : _ Implementations.Direct_stream_writer.t) =
        if is_closed writer
        then
          failwith
            "Rpc.Pipe_rpc.Direct_stream_writer.Group.add_exn: cannot add a closed direct \
             stream writer";
        if Hashtbl.mem t.components_by_id writer.id
        then
          failwith
            "Rpc.Pipe_rpc.Direct_stream_writer.Group.add_exn: trying to add a direct \
             stream writer that is already present in the group";
        (match Bag.choose t.components with
         | None -> ()
         | Some one ->
           let one = Bag.Elt.value one in
           if not (phys_equal (bin_writer one) (bin_writer writer))
           then
             failwith
               "Rpc.Pipe_rpc.Direct_stream_writer.Group.add: cannot add a direct stream \
                writer with a different bin_writer");
        let writer_element_in_group = Bag.add t.components writer in
        let group_element_in_writer =
          Bag.add writer.groups { group = t; element_in_group = writer_element_in_group }
        in
        Hashtbl.add_exn
          t.components_by_id
          ~key:writer.id
          ~data:{ writer_element_in_group; group_element_in_writer };
        if t.send_last_value_on_add
        then (
          (* It shouldn't be possible for last_value_not_written to be Some if there are
             other writers, but in case there is we should send the value in the buffer to
             just this writer and unconditionally reset [last_value_not_written] *)
          (match Moption.is_some t.last_value_not_written, length t with
           | true, 1 ->
             write_without_pushback t (Moption.get_some_exn t.last_value_not_written)
           | (false | true), (_ : int) ->
             if t.last_value_len >= 0
             then (
               (* We already checked if the writer is closed above. *)
               let (`Closed | `Ok) =
                 direct_write_without_pushback
                   writer
                   ~buf:!(t.buffer)
                   ~pos:0
                   ~len:t.last_value_len
               in
               ()));
          Moption.set_none t.last_value_not_written)
      ;;

      let write t x =
        write_without_pushback t x;
        flushed_or_closed t
      ;;
    end
  end

  let implement_direct ~(here : [%call_pos]) ?on_exception ?leave_open_on_exception t f =
    Streaming_rpc.implement_direct ~here ?on_exception ?leave_open_on_exception t f
  ;;

  let implement_direct_with_auth
    ~(here : [%call_pos])
    ?on_exception
    ?leave_open_on_exception
    t
    f
    =
    let f_ok state query writer =
      f state query writer |> Eager_deferred.map ~f:Or_error.return
    in
    Streaming_rpc.implement_direct_with_auth
      ~here
      ?on_exception
      ?leave_open_on_exception
      t
      f_ok
  ;;

  let dispatch_with_metadata' t conn query ~metadata =
    let%map response = Streaming_rpc.dispatch' t conn query ~metadata in
    response >>|~ fun x -> x >>|~ fun (metadata, (), pipe_r) -> pipe_r, metadata
  ;;

  let dispatch' = dispatch_with_metadata' ~metadata:Rpc_metadata.V2.empty

  let dispatch t conn query =
    dispatch' t conn query >>| rpc_result_to_or_error (Streaming_rpc.description t) conn
  ;;

  exception Pipe_rpc_failed

  let dispatch_exn t conn query =
    match%map dispatch t conn query with
    | Error rpc_error -> Error.raise rpc_error
    | Ok (Error _) -> raise Pipe_rpc_failed
    | Ok (Ok pipe_and_id) -> pipe_and_id
  ;;

  let close_reason = Streaming_rpc.Pipe_metadata.close_reason

  let pipe_with_writer_error_of_pipe_and_metadata (pipe, metadata) =
    let writer_error =
      close_reason metadata
      |> Deferred.map ~f:(function
        | Closed_remotely | Closed_locally -> Ok ()
        | Error error -> Error error)
    in
    Pipe_with_writer_error.of_reader ~writer_error pipe
  ;;

  let dispatch_with_close_reason' t connection query =
    dispatch' t connection query
    |> Deferred.Result.map ~f:(Result.map ~f:pipe_with_writer_error_of_pipe_and_metadata)
  ;;

  let dispatch_with_close_reason t connection query =
    dispatch_with_close_reason' t connection query
    >>| rpc_result_to_or_error (Streaming_rpc.description t) connection
  ;;

  module Pipe_message = Pipe_message
  module Pipe_response = Pipe_response

  let dispatch_iter_with_metadata t conn query ~f ~metadata =
    match%map
      Streaming_rpc.dispatch_gen t conn query ~metadata ~initial_handler:(fun () ->
        (), Standard f)
    with
    | (Error _ | Ok (Error _)) as e ->
      rpc_result_to_or_error (Streaming_rpc.description t) conn e
    | Ok (Ok (id, ())) -> Ok (Ok id)
  ;;

  let dispatch_iter = dispatch_iter_with_metadata ~metadata:Rpc_metadata.V2.empty

  module Expert = struct
    let dispatch_bin_prot_with_metadata' = dispatch_with_metadata'
    let dispatch_iter_bin_prot_with_metadata = dispatch_iter_with_metadata

    let dispatch_iter_with_metadata t conn query ~f ~closed ~metadata =
      match%map
        Streaming_rpc.dispatch_gen t conn query ~metadata ~initial_handler:(fun () ->
          (), Expert { on_update = f; on_closed = closed })
      with
      | (Error _ | Ok (Error _)) as e ->
        rpc_result_to_or_error (Streaming_rpc.description t) conn e
      | Ok (Ok (id, ())) -> Ok (Ok id)
    ;;

    let dispatch_iter = dispatch_iter_with_metadata ~metadata:Rpc_metadata.V2.empty
  end

  let abort = Streaming_rpc.abort
  let name = Streaming_rpc.name
  let version = Streaming_rpc.version
  let description = Streaming_rpc.description
  let query_type_id t = t.Streaming_rpc.query_type_id
  let error_type_id t = t.Streaming_rpc.error_response_type_id
  let response_type_id t = t.Streaming_rpc.update_response_type_id
end

module State_rpc = struct
  type ('query, 'state, 'update, 'error) t =
    ('query, 'state, 'update, 'error) Streaming_rpc.t

  module Id = P.Query_id
  module Metadata = Streaming_rpc.Pipe_metadata

  let create
    ?client_pushes_back
    ~name
    ~version
    ~bin_query
    ~bin_state
    ~bin_update
    ~bin_error
    ()
    =
    Streaming_rpc.create
      ?client_pushes_back
      ~name
      ~version
      ~bin_query
      ~bin_initial_response:bin_state
      ~bin_update_response:bin_update
      ~bin_error
      ~alias_for_initial_response:"state"
      ~alias_for_update_response:"update"
      ()
  ;;

  let bin_query t = t.Streaming_rpc.bin_query
  let bin_state t = t.Streaming_rpc.bin_initial_response
  let bin_update t = t.Streaming_rpc.bin_update_response
  let bin_error t = t.Streaming_rpc.bin_error_response
  let shapes t = Streaming_rpc.shapes t
  let implement = Streaming_rpc.implement
  let implement_direct = Streaming_rpc.implement_direct

  let implement_with_auth ~(here : [%call_pos]) ?on_exception ?leave_open_on_exception t f
    =
    let f_ok state query = f state query |> Eager_deferred.map ~f:Or_error.return in
    Streaming_rpc.implement_with_auth ~here ?on_exception ?leave_open_on_exception t f_ok
  ;;

  let implement_direct_with_auth
    ~(here : [%call_pos])
    ?on_exception
    ?leave_open_on_exception
    t
    f
    =
    let f_ok state query writer =
      f state query writer |> Eager_deferred.map ~f:Or_error.return
    in
    Streaming_rpc.implement_direct_with_auth
      ~here
      ?on_exception
      ?leave_open_on_exception
      t
      f_ok
  ;;

  let unwrap_dispatch_result rpc_result =
    let open Result.Let_syntax in
    let%map callee_response = rpc_result in
    let%map metadata, state, update_r = callee_response in
    state, update_r, metadata
  ;;

  let dispatch t conn query =
    Streaming_rpc.dispatch t conn query ~metadata:Rpc_metadata.V2.empty
    >>| unwrap_dispatch_result
  ;;

  let dispatch_with_metadata' t conn query ~metadata =
    Streaming_rpc.dispatch' t conn query ~metadata >>| unwrap_dispatch_result
  ;;

  let dispatch' = dispatch_with_metadata' ~metadata:Rpc_metadata.V2.empty

  let dispatch_with_close_reason' t conn query =
    Deferred.Result.map (dispatch' t conn query) ~f:(fun result ->
      Result.map result ~f:(fun (state, update_r, metadata) ->
        let pipe_with_writer_error =
          Pipe_rpc.pipe_with_writer_error_of_pipe_and_metadata (update_r, metadata)
        in
        state, pipe_with_writer_error))
  ;;

  let dispatch_with_close_reason t conn query =
    dispatch_with_close_reason' t conn query
    >>| rpc_result_to_or_error (Streaming_rpc.description t) conn
  ;;

  module Pipe_message = Pipe_message
  module Pipe_response = Pipe_response

  let dispatch_fold = Streaming_rpc.dispatch_fold ~metadata:Rpc_metadata.V2.empty
  let dispatch_fold_with_metadata = Streaming_rpc.dispatch_fold

  module Expert = struct
    let dispatch_fold_bin_prot_with_metadata = dispatch_fold_with_metadata
    let dispatch_bin_prot_with_metadata' = dispatch_with_metadata'
  end

  let abort = Streaming_rpc.abort
  let close_reason = Streaming_rpc.Pipe_metadata.close_reason
  let client_pushes_back t = t.Streaming_rpc.client_pushes_back
  let name = Streaming_rpc.name
  let version = Streaming_rpc.version
  let description = Streaming_rpc.description
  let query_type_id t = t.Streaming_rpc.query_type_id
  let state_type_id t = t.Streaming_rpc.initial_response_type_id
  let update_type_id t = t.Streaming_rpc.update_response_type_id
  let error_type_id t = t.Streaming_rpc.error_response_type_id
end

module Any = struct
  type t =
    | Rpc : ('q, 'r) Rpc.t -> t
    | Pipe : ('q, 'r, 'e) Pipe_rpc.t -> t
    | State : ('q, 's, 'u, 'e) State_rpc.t -> t
    | One_way : 'm One_way.t -> t

  let description = function
    | Rpc rpc -> Rpc.description rpc
    | Pipe rpc -> Pipe_rpc.description rpc
    | State rpc -> State_rpc.description rpc
    | One_way rpc -> One_way.description rpc
  ;;
end

module Stable = struct
  module Description = Description.Stable
  module Pipe_close_reason = Pipe_close_reason.Stable
  module Rpc = Rpc
  module Pipe_rpc = Pipe_rpc
  module State_rpc = State_rpc
  module One_way = One_way
end
