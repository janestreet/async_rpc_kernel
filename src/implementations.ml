open Core_kernel.Std
open Async_kernel.Std
open Util

module P = Protocol
module Reader = Transport.Reader
module Writer = Transport.Writer

(* The Result monad is also used. *)
let (>>|~) = Result.(>>|)

(* Commute Result and Deferred. *)
let defer_result : 'a 'b. ('a Deferred.t,'b) Result.t -> ('a,'b) Result.t Deferred.t =
  function
  | Error _ as err -> return err
  | Ok d ->
    match Deferred.peek d with
    | None -> d >>| fun x -> Ok x
    | Some d -> return (Ok d)

module Responder = struct
  type t = P.Query_id.t * Writer.t
end

type 'connection_state on_unknown_rpc =
  [ `Raise
  | `Continue
  | `Close_connection
  | `Call of
      ('connection_state
       -> rpc_tag : string
       -> version : int
       -> [ `Close_connection | `Continue ])
  | `Expert of
      ('connection_state
       -> rpc_tag : string
       -> version : int
       -> Responder.t
       -> Bigstring.t
       -> pos : int
       -> len : int
       -> unit Deferred.t)
  ]

type 'connection_state t =
  { implementations : 'connection_state Implementation.F.t Description.Table.t
  ; on_unknown_rpc  : 'connection_state on_unknown_rpc
  }

let descriptions t = Hashtbl.keys t.implementations

module Instance = struct
  type a_pipe = Pipe : _ Pipe.Reader.t -> a_pipe with sexp_of

  type streaming_response =
    { abort : unit Ivar.t
    (* We create a [streaming_response] as soon as a streaming RPC query comes in, but we
       don't get the pipe until later. *)
    ; pipe  : a_pipe Set_once.t
    } with sexp_of

  type streaming_responses = (P.Query_id.t, streaming_response) Hashtbl.t with sexp_of

  type 'a unpacked =
    { implementations          : 'a t sexp_opaque
    ; writer                   : Writer.t
    ; open_streaming_responses : streaming_responses
    ; mutable stopped          : bool
    ; connection_state         : 'a
    ; connection_description   : Info.t
    ; mutable last_dispatched_implementation
      : (Description.t * 'a Implementation.F.t sexp_opaque) option
    } with sexp_of

  let write_response t ~(query : Nat0.t P.Query.t) bin_writer_data data =
    if (not t.stopped) && (not (Writer.is_closed t.writer))
    then
      let bin_writer =
        P.Message.bin_writer_needs_length (Writer_with_length.of_writer bin_writer_data)
      in
      Writer.send_bin_prot t.writer bin_writer (Response { id = query.id; data })
  ;;

  let apply_implementation
        t
        implementation
        ~(query : Nat0.t P.Query.t)
        ~read_buffer
        ~read_buffer_pos_ref
        ~aborted
    : _ Transport.Handler_result.t =
    match implementation with
    | Implementation.F.One_way (bin_query_reader, f) ->
      let query_contents =
        bin_read_from_bigstring bin_query_reader read_buffer ~pos_ref:read_buffer_pos_ref
          ~len:query.data
          ~location:"server-side one-way rpc message un-bin-io'ing"
      in
      (match query_contents with
       | Error _ as err ->
         Stop err
       | Ok q ->
         try
           f t.connection_state q;
           Continue
         with exn ->
           Stop
             (Rpc_result.uncaught_exn exn
                ~location:"server-side one-way rpc computation")
      )
    | Implementation.F.One_way_expert f ->
      (try
         let len = (query.data :> int) in
         f t.connection_state read_buffer ~pos:!read_buffer_pos_ref ~len;
         read_buffer_pos_ref := !read_buffer_pos_ref + len;
         Continue
       with exn ->
         Stop
           (Rpc_result.uncaught_exn exn
              ~location:"server-side one-way rpc expert computation")
      )
    | Implementation.F.Blocking_rpc (bin_query_reader, bin_response_writer, f) ->
      let query_contents =
        bin_read_from_bigstring bin_query_reader read_buffer
          ~pos_ref:read_buffer_pos_ref
          ~len:query.data
          ~location:"server-side rpc query un-bin-io'ing"
      in
      let data =
        try query_contents >>|~ f t.connection_state with
        | exn ->
          Rpc_result.uncaught_exn
            ~location:"server-side blocking rpc computation"
            exn
      in
      write_response t ~query bin_response_writer data;
      Continue
    | Implementation.F.Rpc (bin_query_reader, bin_response_writer, f) ->
      let query_contents =
        bin_read_from_bigstring bin_query_reader read_buffer
          ~pos_ref:read_buffer_pos_ref
          ~len:query.data
          ~location:"server-side rpc query un-bin-io'ing"
      in
      let data =
        Rpc_result.try_with ~run:`Now
          ~location:"server-side rpc computation" (fun () ->
            defer_result (query_contents >>|~ f t.connection_state))
      in
      (* In the common case that the implementation returns a value immediately, we will
         write the response immediately as well (this is also why the above [try_with] has
         [~run:`Now]).  This can be a big performance win for servers that get many
         queries in a single Async cycle. *)
      ( match Deferred.peek data with
        | None -> data >>> write_response t ~query bin_response_writer
        | Some data -> write_response t ~query bin_response_writer data );
      Continue
    | Implementation.F.Pipe_rpc
        (bin_query_reader, bin_init_writer, bin_update_writer, f) ->
      let stream_query =
        bin_read_from_bigstring P.Stream_query.bin_reader_nat0_t
          read_buffer ~pos_ref:read_buffer_pos_ref
          ~len:query.data
          ~location:"server-side pipe_rpc stream_query un-bin-io'ing"
          ~add_len:(function `Abort -> 0 | `Query (len : Nat0.t) -> (len :> int))
      in
      begin
        match stream_query with
        | Error _err -> ()
        | Ok `Abort ->
          Option.iter (Hashtbl.find t.open_streaming_responses query.id)
            ~f:(fun resp -> Ivar.fill_if_empty resp.abort ());
        | Ok (`Query len) ->
          let data =
            bin_read_from_bigstring bin_query_reader read_buffer
              ~pos_ref:read_buffer_pos_ref ~len
              ~location:"streaming_rpc server-side query un-bin-io'ing"
          in
          let streaming_response : streaming_response =
            { abort = Ivar.create ()
            ; pipe  = Set_once.create ()
            }
          in
          Hashtbl.set t.open_streaming_responses ~key:query.id ~data:streaming_response;
          let aborted = Deferred.any [
            Ivar.read streaming_response.abort;
            aborted;
          ]
          in
          let data = Rpc_result.try_with (fun () -> defer_result (
            data >>|~ fun data ->
            f t.connection_state data ~aborted
          )) ~location:"server-side pipe_rpc computation"
          in
          data >>> fun data ->
          let remove_streaming_response () =
            Hashtbl.remove t.open_streaming_responses query.id
          in
          match data with
          | Error err ->
            remove_streaming_response ();
            write_response t ~query bin_init_writer (Error err)
          | Ok (Error err) ->
            remove_streaming_response ();
            write_response t ~query bin_init_writer (Ok err)
          | Ok (Ok (initial, pipe_r)) ->
            Set_once.set_exn streaming_response.pipe (Pipe pipe_r);
            write_response t ~query bin_init_writer (Ok initial);
            let bin_update_writer =
              P.Stream_response_data.bin_writer_needs_length
                (Writer_with_length.of_writer bin_update_writer)
            in
            don't_wait_for
              (Writer.transfer t.writer pipe_r (fun x ->
                 write_response t ~query bin_update_writer (Ok (`Ok x))));
            Pipe.closed pipe_r >>> fun () ->
            Pipe.upstream_flushed pipe_r
            >>> function
            | `Ok | `Reader_closed ->
              write_response t ~query bin_update_writer (Ok `Eof);
              remove_streaming_response ()
      end;
      Continue

  type t = T : _ unpacked -> t
  let sexp_of_t (T t) = <:sexp_of< _ unpacked >> t

  let flush (T t) =
    assert (not t.stopped);
    let producers_flushed =
      Hashtbl.fold t.open_streaming_responses ~init:[]
        ~f:(fun ~key:_ ~data:{ pipe; _ } acc ->
          match Set_once.get pipe with
          | None             -> acc
          | Some (Pipe pipe) -> Deferred.ignore (Pipe.upstream_flushed pipe) :: acc)
    in
    Deferred.all_unit producers_flushed
  ;;

  let stop (T t) = t.stopped <- true

  let handle_unknown_rpc on_unknown_rpc error t query : _ Transport.Handler_result.t =
    match on_unknown_rpc with
    | `Continue         -> Continue
    | `Raise            -> Rpc_error.raise error t.connection_description
    | `Close_connection -> Stop (Ok ())
    | `Call f ->
      match
        f t.connection_state ~rpc_tag:(P.Rpc_tag.to_string query.P.Query.tag)
          ~version:query.version
      with
      | `Close_connection -> Stop (Ok ())
      | `Continue         -> Continue
  ;;

  let handle_query_internal t ~(query : Nat0.t P.Query.t) ~aborted ~read_buffer
        ~read_buffer_pos_ref =
    let { implementations; on_unknown_rpc } = t.implementations in
    let description : Description.t =
      { name = P.Rpc_tag.to_string query.tag; version = query.version }
    in
    match t.last_dispatched_implementation with
    | Some (last_desc, implementation) when Description.equal last_desc description ->
      apply_implementation t implementation  ~query ~read_buffer ~read_buffer_pos_ref
        ~aborted
    | None | Some _ ->
      match Hashtbl.find implementations description with
      | Some implementation ->
        t.last_dispatched_implementation <- Some (description, implementation);
        apply_implementation t implementation ~query ~read_buffer ~read_buffer_pos_ref
          ~aborted
      | None ->
        match on_unknown_rpc with
        | `Expert impl ->
          let {P.Query.tag; version; id; data = len} = query in
          let d =
            impl t.connection_state ~rpc_tag:(P.Rpc_tag.to_string tag) ~version
              (id, t.writer) read_buffer ~pos:!read_buffer_pos_ref
              ~len:(len :> int)
          in
          if Deferred.is_determined d
          then Continue
          else Wait d
        | (`Continue | `Raise | `Close_connection | `Call _) as on_unknown_rpc ->
          let error = Rpc_error.Unimplemented_rpc (query.tag, `Version query.version) in
          Writer.send_bin_prot t.writer P.Message.bin_writer_nat0_t
            (Response
               { id   = query.id
               ; data = Error error
               });
          handle_unknown_rpc on_unknown_rpc error t query
  ;;

  let handle_query (T t) ~query ~aborted ~read_buffer ~read_buffer_pos_ref =
    assert (not t.stopped);
    if Writer.is_closed t.writer then
      Transport.Handler_result.Stop (Ok ())
    else
      handle_query_internal t ~query ~aborted ~read_buffer ~read_buffer_pos_ref
  ;;
end

let create ~implementations:i's ~on_unknown_rpc =
  (* Make sure the tags are unique. *)
  let implementations = Description.Table.create ~size:10 () in
  let dups = Description.Hash_set.create ~size:10 () in
  List.iter i's ~f:(fun (i : _ Implementation.t) ->
    let description =
      { Description.
        name    = P.Rpc_tag.to_string i.tag
      ; version = i.version
      }
    in
    match Hashtbl.add implementations ~key:description ~data:i.f with
    | `Ok -> ()
    | `Duplicate -> Hash_set.add dups description
  );
  if not (Hash_set.is_empty dups) then
    Error (`Duplicate_implementations (Hash_set.to_list dups))
  else
    Ok {
      implementations;
      on_unknown_rpc = (on_unknown_rpc :> _ on_unknown_rpc);
    }

let instantiate t ~connection_description ~connection_state ~writer =
  Instance.T
    { implementations = t
    ; writer
    ; open_streaming_responses = Hashtbl.Poly.create ~size:10 ()
    ; connection_state
    ; connection_description
    ; stopped = false
    ; last_dispatched_implementation = None
    }

exception Duplicate_implementations of Description.t list with sexp

let create_exn ~implementations ~on_unknown_rpc =
  match create ~implementations ~on_unknown_rpc with
  | Ok x -> x
  | Error `Duplicate_implementations dups -> raise (Duplicate_implementations dups)

let null () = create_exn ~implementations:[] ~on_unknown_rpc:`Raise

let add_exn t (implementation : _ Implementation.t) =
  let desc : Description.t =
    { name = P.Rpc_tag.to_string implementation.tag
    ; version = implementation.version
    }
  in
  let implementations = Hashtbl.copy t.implementations in
  match Hashtbl.add implementations ~key:desc ~data:implementation.f with
  | `Duplicate -> raise (Duplicate_implementations [desc])
  | `Ok -> { t with implementations }

let add t implementation =
  Or_error.try_with (fun () -> add_exn t implementation)


let lift {implementations; on_unknown_rpc} ~f =
  let implementations =
    Hashtbl.map implementations ~f:(Implementation.F.lift ~f)
  in
  let on_unknown_rpc =
    match on_unknown_rpc with
    | `Raise | `Continue | `Close_connection as x -> x
    | `Call call -> `Call (fun state -> call (f state))
    | `Expert expert -> `Expert (fun state -> expert (f state))
  in
  { implementations; on_unknown_rpc }

module Expert = struct
  module Responder = Responder

  module Rpc_responder = struct
    type t = Responder.t

    let schedule (id, w) buf ~pos ~len =
      let header : Nat0.t P.Message.t = Response {id; data = Ok (Nat0.of_int_exn len)} in
      if Writer.is_closed w then
        `Connection_closed
      else
        `Flushed
          (Writer.send_bin_prot_and_bigstring_non_copying w
             P.Message.bin_writer_nat0_t header
             ~buf ~pos ~len)

    let write_bigstring (id, w) buf ~pos ~len =
      let header : Nat0.t P.Message.t = Response {id; data = Ok (Nat0.of_int_exn len)} in
      if not (Writer.is_closed w) then
        Writer.send_bin_prot_and_bigstring w
          P.Message.bin_writer_nat0_t header
          ~buf ~pos ~len

    let write_error (id, w) error =
      if not (Writer.is_closed w) then
        let data =
          Rpc_result.uncaught_exn ~location:"server-side raw rpc computation"
            (Error.to_exn error)
        in
        Writer.send_bin_prot w P.Message.bin_writer_nat0_t (Response {id; data})

    let write_bin_prot (id, w) bin_writer_a a =
      if not (Writer.is_closed w) then
        Writer.send_bin_prot w
          (P.Message.bin_writer_needs_length (Writer_with_length.of_writer bin_writer_a))
          (Response {id; data = Ok a})
  end

  let create_exn = create_exn
end
