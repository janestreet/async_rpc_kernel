
open Core_kernel.Std
open Async_kernel.Std
open Util

module P = Protocol

module Description     = Description
module Implementation  = Implementation
module Implementations = Implementations
module Transport       = Transport
module Connection      = Connection

(* The Result monad is also used. *)
let (>>=~) = Result.(>>=)
let (>>|~) = Result.(>>|)

module Rpc_common = struct

  let dispatch_raw' conn ~tag ~version ~bin_writer_query ~query ~query_id
        ~response_handler =
    let query =
      { P.Query.
        tag
      ; version
      ; id      = query_id
      ; data    = query
      }
    in
    match Connection.dispatch conn ~response_handler ~bin_writer_query ~query with
    | Ok () -> Ok ()
    | Error `Closed -> Error Rpc_error.Connection_closed

  let dispatch_raw conn ~tag ~version ~bin_writer_query ~query ~query_id ~f =
    let response_ivar = Ivar.create () in
    begin
      match
        dispatch_raw' conn ~tag ~version ~bin_writer_query ~query ~query_id
          ~response_handler:(Some (f response_ivar))
      with
      | Ok () -> ()
      | Error _ as e -> Ivar.fill response_ivar e
    end;
    Ivar.read response_ivar
    >>| Rpc_result.or_error
          ~rpc_tag:tag
          ~rpc_version:version
          ~connection_description:(Connection.description conn)

end

module Rpc = struct
  type ('query,'response) t =
    { tag          : P.Rpc_tag.t
    ; version      : int
    ; bin_query    : 'query    Bin_prot.Type_class.t
    ; bin_response : 'response Bin_prot.Type_class.t
    }

  let create ~name ~version ~bin_query ~bin_response =
    { tag          = P.Rpc_tag.of_string name
    ; version
    ; bin_query
    ; bin_response
    }

  let name t = P.Rpc_tag.to_string t.tag
  let version t = t.version

  let description t =
    { Description.
      name    = name t
    ; version = version t
    }

  let bin_query t = t.bin_query
  let bin_response t = t.bin_response

  let implement t f =
    { Implementation.
      tag     = t.tag
    ; version = t.version
    ; f       = Rpc (t.bin_query.reader, t.bin_response.writer, f, Deferred)
    }

  let implement' t f =
    { Implementation.
      tag     = t.tag
    ; version = t.version
    ; f       = Rpc (t.bin_query.reader, t.bin_response.writer, f, Blocking)
    }

  let dispatch t conn query =
    let response_handler ivar =
      fun (response : _ P.Response.t) ~read_buffer ~read_buffer_pos_ref ->
        let response =
          response.data >>=~ fun len ->
          bin_read_from_bigstring t.bin_response.reader
            read_buffer ~pos_ref:read_buffer_pos_ref ~len
            ~location:"client-side rpc response un-bin-io'ing"
        in
        Ivar.fill ivar response;
        `remove (Ok ())
    in
    let query_id = P.Query_id.create () in
    Rpc_common.dispatch_raw conn ~tag:t.tag ~version:t.version
      ~bin_writer_query:t.bin_query.writer ~query ~query_id
      ~f:response_handler

  let dispatch_exn t conn query = dispatch t conn query >>| Or_error.ok_exn

  module Expert = struct
    module Responder = Implementations.Expert.Rpc_responder

    let make_dispatch do_dispatch conn ~rpc_tag ~version buf ~pos ~len ~handle_response
          ~handle_error
      =
      let response_handler : Connection.response_handler =
        fun response ~read_buffer ~read_buffer_pos_ref ->
          match response.data with
          | Error e ->
            handle_error (Error.t_of_sexp (Rpc_error.sexp_of_t e));
            `remove (Ok ())
          | Ok len ->
            let len = (len : Nat0.t :> int) in
            let d = handle_response read_buffer ~pos:!read_buffer_pos_ref ~len in
            read_buffer_pos_ref := !read_buffer_pos_ref + len;
            if Deferred.is_determined d
            then `remove (Ok ())
            else `remove_and_wait d
      in
      do_dispatch conn ~tag:(P.Rpc_tag.of_string rpc_tag) ~version
        buf ~pos ~len ~response_handler:(Some response_handler)

    let dispatch conn ~rpc_tag ~version buf ~pos ~len ~handle_response
          ~handle_error
      =
      match
        make_dispatch Connection.dispatch_bigstring
          conn ~rpc_tag ~version buf ~pos ~len ~handle_response ~handle_error
      with
      | Ok () -> `Ok
      | Error `Closed -> `Connection_closed

    let schedule_dispatch conn ~rpc_tag ~version buf ~pos ~len ~handle_response
          ~handle_error
      =
      match
        make_dispatch Connection.schedule_dispatch_bigstring
          conn ~rpc_tag ~version buf ~pos ~len ~handle_response ~handle_error
      with
      | Ok d          -> `Flushed d
      | Error `Closed -> `Connection_closed

    type implementation_result = Implementation.Expert.implementation_result =
      | Replied
      | Delayed_response of unit Deferred.t

    let implement t f =
      { Implementation.
        tag     = t.tag
      ; version = t.version
      ; f       = Rpc_expert (f, Deferred)
      }

    let implement' t f =
      { Implementation.
        tag     = t.tag
      ; version = t.version
      ; f       = Rpc_expert (f, Blocking)
      }
  end
end

module One_way = struct
  type 'msg t =
    { tag     : P.Rpc_tag.t
    ; version : int
    ; bin_msg : 'msg Bin_prot.Type_class.t
    } [@@deriving fields]

  let name t = P.Rpc_tag.to_string t.tag

  let create ~name ~version ~bin_msg =
    { tag = P.Rpc_tag.of_string name
    ; version
    ; bin_msg
    }

  let description t =
    { Description.
      name    = name t
    ; version = version t
    }

  let implement t f =
    { Implementation.
      tag     = t.tag
    ; version = t.version
    ; f       = One_way (t.bin_msg.reader, f)
    }

  let dispatch t conn query =
    let query_id = P.Query_id.create () in
    Rpc_common.dispatch_raw' conn ~tag:t.tag ~version:t.version
      ~bin_writer_query:t.bin_msg.writer ~query ~query_id ~response_handler:None
    |> Rpc_result.or_error
         ~rpc_tag:t.tag
         ~rpc_version:t.version
         ~connection_description:(Connection.description conn)

  let dispatch_exn t conn query = Or_error.ok_exn (dispatch t conn query)

  module Expert = struct
    let implement t f =
      { Implementation.
        tag = t.tag;
        version = t.version;
        f = One_way_expert f;
      }

    let dispatch {tag; version; bin_msg = _} conn buf ~pos ~len =
      match
        Connection.dispatch_bigstring
          conn ~tag ~version buf ~pos ~len ~response_handler:None
      with
      | Ok () -> `Ok
      | Error `Closed -> `Connection_closed

    let schedule_dispatch {tag; version; bin_msg = _} conn buf ~pos ~len =
      match
        Connection.schedule_dispatch_bigstring
          conn ~tag ~version buf ~pos ~len ~response_handler:None
      with
      | Ok flushed -> `Flushed flushed
      | Error `Closed -> `Connection_closed
  end
end

module Pipe_close_reason = struct
  type t =
    | Closed_locally
    | Closed_remotely
    | Error of Error.t
end

(* the basis of the implementations of Pipe_rpc and State_rpc *)
module Streaming_rpc = struct
  module Initial_message = P.Stream_initial_message

  type ('query, 'initial_response, 'update_response, 'error_response) t =
    { tag                  : P.Rpc_tag.t
    ; version              : int
    ; bin_query            : 'query            Bin_prot.Type_class.t
    ; bin_initial_response : 'initial_response Bin_prot.Type_class.t
    ; bin_update_response  : 'update_response  Bin_prot.Type_class.t
    ; bin_error_response   : 'error_response   Bin_prot.Type_class.t
    ; client_pushes_back   : bool
    }

  let create ?client_pushes_back ~name ~version ~bin_query ~bin_initial_response
        ~bin_update_response ~bin_error () =
    let client_pushes_back =
      match client_pushes_back with
      | None -> false
      | Some () -> true
    in
    { tag                  = P.Rpc_tag.of_string name
    ; version
    ; bin_query
    ; bin_initial_response
    ; bin_update_response
    ; bin_error_response   = bin_error
    ; client_pushes_back
    }

  let make_initial_message x =
    { Initial_message.
      unused_query_id = P.Unused_query_id.t
    ; initial         = x
    }

  let implement_gen t impl =
    let bin_init_writer =
      Initial_message.bin_writer_t
        t.bin_initial_response.writer
        t.bin_error_response.writer
    in
    { Implementation.
      tag     = t.tag
    ; version = t.version
    ; f =
        Streaming_rpc (
          t.bin_query.reader,
          bin_init_writer,
          t.bin_update_response.writer,
          impl
        );
    }
  ;;

  let implement t f =
    let f c query =
      f c query
      >>| function
      | Error err -> Error (make_initial_message (Error err))
      | Ok (initial, pipe) -> Ok (make_initial_message (Ok initial), pipe)
    in
    implement_gen t (Pipe f)
  ;;

  let implement_direct t f =
    let f c query writer =
      f c query writer
      >>| function
      | Error _ as x -> Error (make_initial_message x)
      | Ok    _ as x -> Ok    (make_initial_message x)
    in
    implement_gen t (Direct f)
  ;;

  let abort t conn id =
    let query =
      { P.Query.
        tag     = t.tag
      ; version = t.version
      ; id
      ; data = `Abort
      }
    in
    ignore (
      Connection.dispatch conn ~bin_writer_query:P.Stream_query.bin_writer_nat0_t ~query
        ~response_handler:None : (unit,[`Closed]) Result.t
    )

  module Pipe_message = struct
    type 'a t =
      | Update of 'a
      | Closed of [`By_remote_side | `Error of Error.t]
  end

  module Pipe_response = struct
    type t =
      | Continue
      | Wait of unit Deferred.t
  end

  module Pipe_metadata = struct
    type t = {
      query_id : P.Query_id.t;
      close_reason : Pipe_close_reason.t Deferred.t;
    }

    let id t = t.query_id

    let close_reason t = t.close_reason
  end

  module Response_state = struct
    module Update_handler = struct
      type 'a t = 'a Pipe_message.t -> Pipe_response.t
    end

    module Initial = struct
      type nonrec ('q, 'i, 'u, 'e, 'extra) t = {
        rpc : ('q, 'i, 'u, 'e) t;
        query_id : P.Query_id.t;
        make_update_handler : (unit -> 'extra * 'u Update_handler.t);
        ivar : (P.Query_id.t * 'i * 'extra, 'e) Result.t Rpc_result.t Ivar.t;
        connection : Connection.t;
      }
    end

    module State = struct
      type 'a t =
        | Waiting_for_initial_response : ('q, 'i, 'u, 'e, 'extra) Initial.t -> 'u t
        | Writing_updates of 'a Bin_prot.Type_class.reader * 'a Update_handler.t
    end

    type 'a t = { mutable state : 'a State.t }
  end

  let read_error (handler : _ Response_state.Update_handler.t) err =
    let core_err = Error.t_of_sexp (Rpc_error.sexp_of_t err) in
    ignore (handler (Closed (`Error core_err)) : Pipe_response.t);
    `remove (Error err)

  let eof (handler : _ Response_state.Update_handler.t) =
    ignore (handler (Closed `By_remote_side) : Pipe_response.t);
    `remove (Ok ())

  let response_handler initial_state : Connection.response_handler =
    let open Response_state in
    let state = { state = Waiting_for_initial_response initial_state } in
    fun response ~read_buffer ~read_buffer_pos_ref ->
      match state.state with
      | Writing_updates (bin_reader_update, handler) ->
        begin match response.data with
        | Error err ->
          read_error handler err
        | Ok len ->
          let data =
            bin_read_from_bigstring
              P.Stream_response_data.bin_reader_nat0_t
              read_buffer ~pos_ref:read_buffer_pos_ref ~len
              ~location:"client-side streaming_rpc response un-bin-io'ing"
              ~add_len:(function `Eof -> 0 | `Ok (len : Nat0.t) -> (len :> int))
          in
          match data with
          | Error err ->
            read_error handler err
          | Ok `Eof ->
            eof handler
          | Ok (`Ok len) ->
            let data =
              bin_read_from_bigstring bin_reader_update
                read_buffer ~pos_ref:read_buffer_pos_ref ~len
                ~location:"client-side streaming_rpc response un-bin-io'ing"
            in
            match data with
            | Error err ->
              read_error handler err
            | Ok data ->
              match handler (Update data) with
              | Continue -> `keep
              | Wait d -> `wait d
        end
      | State.Waiting_for_initial_response initial_handler ->
        (* We never use [`remove (Error _)] here, since that indicates that the
           connection should be closed, and these are "normal" errors. (In contrast, the
           errors we get in the [Writing_updates_to_pipe] case indicate more serious
           problems.) Instead, we just put errors in [ivar]. *)
        let error err =
          Ivar.fill initial_handler.ivar (Error err);
          `remove (Ok ())
        in
        begin match response.data with
        | Error err -> error err
        | Ok len ->
          let initial =
            bin_read_from_bigstring
              (Initial_message.bin_reader_t
                 initial_handler.rpc.bin_initial_response.reader
                 initial_handler.rpc.bin_error_response.reader)
              read_buffer ~pos_ref:read_buffer_pos_ref ~len
              ~location:"client-side streaming_rpc initial_response un-bin-io'ing"
          in
          begin match initial with
          | Error err -> error err
          | Ok initial_msg ->
            begin match initial_msg.initial with
            | Error err ->
              Ivar.fill initial_handler.ivar (Ok (Error err));
              `remove (Ok ())
            | Ok initial ->
              let (extra, handler) = initial_handler.make_update_handler () in
              Ivar.fill initial_handler.ivar
                (Ok (Ok (initial_handler.query_id, initial, extra)));
              state.state <-
                Writing_updates (initial_handler.rpc.bin_update_response.reader, handler);
              `keep
            end
          end
        end
  ;;

  let dispatch_gen t conn query make_update_handler =
    let bin_writer_query =
      P.Stream_query.bin_writer_needs_length (Writer_with_length.of_type_class t.bin_query)
    in
    let query = `Query query in
    let query_id = P.Query_id.create () in
    Rpc_common.dispatch_raw conn ~query_id ~tag:t.tag ~version:t.version
      ~bin_writer_query ~query
      ~f:(fun ivar ->
        response_handler {
          rpc = t;
          query_id;
          connection = conn;
          ivar;
          make_update_handler;
        })
  ;;

  let dispatch_iter t conn query ~f =
    dispatch_gen t conn query (fun () -> (), f)
    >>| function
    | Error _ | Ok (Error _) as e -> e
    | Ok (Ok (id, init, ())) -> Ok (Ok (id, init))
  ;;

  let dispatch t conn query =
    dispatch_gen t conn query (fun () ->
      let (pipe_r, pipe_w) = Pipe.create () in
      (* Set a small buffer to reduce the number of pushback events *)
      Pipe.set_size_budget pipe_w 100;
      let close_reason : Pipe_close_reason.t Ivar.t = Ivar.create () in
      let f : _ Response_state.Update_handler.t = function
        | Update data ->
          if not (Pipe.is_closed pipe_w) then begin
            Pipe.write_without_pushback pipe_w data;
            if t.client_pushes_back && Pipe.length pipe_w = Pipe.size_budget pipe_w then
              Wait
                (Pipe.downstream_flushed pipe_w
                 >>| function
                 | `Ok
                 | `Reader_closed -> ())
            else
              Continue
          end else
            Continue
        | Closed reason ->
          Ivar.fill_if_empty close_reason
            (match reason with
             | `By_remote_side -> Closed_remotely
             | `Error err -> Error err);
          Pipe.close pipe_w;
          Continue
      in
      ((pipe_r, close_reason), f)
    )
    >>| function
    | Error _ | Ok (Error _) as e -> e
    | Ok (Ok (id, init, (pipe_r, close_reason))) ->
      upon (Pipe.closed pipe_r) (fun () ->
        if not (Ivar.is_full close_reason) then begin
          abort t conn id;
          Ivar.fill close_reason Closed_locally;
        end);
      let pipe_metadata : Pipe_metadata.t =
        { query_id = id;
          close_reason = Ivar.read close_reason;
        }
      in
      Ok (Ok (pipe_metadata, init, pipe_r))
  ;;
end

(* A Pipe_rpc is like a Streaming_rpc, except we don't care about initial state - thus
   it is restricted to unit and ultimately ignored *)
module Pipe_rpc = struct
  type ('query, 'response, 'error) t = ('query, unit, 'response, 'error) Streaming_rpc.t

  module Id = P.Query_id
  module Metadata = Streaming_rpc.Pipe_metadata

  let create ?client_pushes_back ~name ~version ~bin_query ~bin_response ~bin_error () =
    Streaming_rpc.create
      ?client_pushes_back
      ~name ~version ~bin_query
      ~bin_initial_response: Unit.bin_t
      ~bin_update_response:  bin_response
      ~bin_error
      ()

  let bin_query t = t.Streaming_rpc.bin_query
  let bin_response t = t.Streaming_rpc.bin_update_response
  let bin_error t = t.Streaming_rpc.bin_error_response

  let implement t f =
    Streaming_rpc.implement t (fun a query ->
      f a query >>| fun x ->
      x >>|~ fun x -> (), x
    )

  module Direct_stream_writer = Implementations.Direct_stream_writer

  let implement_direct t f = Streaming_rpc.implement_direct t f

  let dispatch t conn query =
    Streaming_rpc.dispatch t conn query >>| fun response ->
    response >>|~ fun x ->
    x >>|~ fun (query_id, (), pipe_r) ->
    pipe_r, query_id

  exception Pipe_rpc_failed

  let dispatch_exn t conn query =
    dispatch t conn query
    >>| fun result ->
    match result with
    | Error rpc_error -> raise (Error.to_exn rpc_error)
    | Ok (Error _) -> raise Pipe_rpc_failed
    | Ok (Ok pipe_and_id) -> pipe_and_id

  module Pipe_message = Streaming_rpc.Pipe_message
  module Pipe_response = Streaming_rpc.Pipe_response

  let dispatch_iter t conn query ~f =
    Streaming_rpc.dispatch_iter t conn query ~f >>| fun response ->
    response >>|~ fun x ->
    x >>|~ fun (id, ()) ->
    id

  let abort = Streaming_rpc.abort
  let close_reason = Streaming_rpc.Pipe_metadata.close_reason

  let name t = P.Rpc_tag.to_string t.Streaming_rpc.tag
  let version t = t.Streaming_rpc.version

  let description t =
    { Description.
      name    = name t
    ; version = version t
    }
end

module State_rpc = struct
  type ('query, 'initial, 'response, 'error) t =
    ('query, 'initial, 'response, 'error) Streaming_rpc.t

  module Id = P.Query_id
  module Metadata = Streaming_rpc.Pipe_metadata

  let create ?client_pushes_back ~name ~version ~bin_query ~bin_state ~bin_update
        ~bin_error () =
    Streaming_rpc.create
      ?client_pushes_back
      ~name ~version ~bin_query
      ~bin_initial_response:bin_state
      ~bin_update_response:bin_update
      ~bin_error
      ()

  let bin_query  t = t.Streaming_rpc.bin_query
  let bin_state  t = t.Streaming_rpc.bin_initial_response
  let bin_update t = t.Streaming_rpc.bin_update_response
  let bin_error  t = t.Streaming_rpc.bin_error_response

  let implement = Streaming_rpc.implement

  let folding_map input_r ~init ~f =
    let state = ref init in
    Pipe.map input_r
      ~f:(fun update ->
        let state' = f !state update in
        state := state';
        state', update)

  let dispatch t conn query ~update =
    Streaming_rpc.dispatch t conn query >>| fun response ->
    response >>|~ fun x ->
    x >>|~ fun (id, state, update_r) ->
    state,
    folding_map update_r ~init:state ~f:update,
    id

  let abort = Streaming_rpc.abort
  let close_reason = Streaming_rpc.Pipe_metadata.close_reason

  let name t = P.Rpc_tag.to_string t.Streaming_rpc.tag
  let version t = t.Streaming_rpc.version

  let description t =
    { Description.
      name = name t
    ; version = version t
    }
end

module Any = struct
  type t =
    | Rpc     : ('q, 'r) Rpc.t -> t
    | Pipe    : ('q, 'r, 'e) Pipe_rpc.t -> t
    | State   : ('q, 's, 'u, 'e) State_rpc.t -> t
    | One_way : 'm One_way.t -> t
end
