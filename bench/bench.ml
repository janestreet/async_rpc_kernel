open Core
open Async

module%bench [@name "direct stream writer"] _ = struct
  let max_message_size = 16 lsl 20

  let config =
    Rpc.Low_latency_transport.Config.create
    (* Always batch, we don't care about measuring the syscall time here *)
      ~start_batching_after_num_messages:0
      ()
  ;;

  let plain_rpc =
    Rpc.Rpc.create
      ~name:"plain-rpc"
      ~version:1
      ~bin_query:[%bin_type_class: unit]
      ~bin_response:[%bin_type_class: int]
      ~include_in_error_count:Only_on_exn
  ;;

  let basic_rpc name =
    Rpc.Pipe_rpc.create
      ~name
      ~version:42
      ~bin_query:[%bin_type_class: unit]
      ~bin_response:[%bin_type_class: int * int]
      ~bin_error:[%bin_type_class: unit]
      ()
  ;;

  let big_rpc name =
    Rpc.Pipe_rpc.create
      ~name
      ~version:42
      ~bin_query:[%bin_type_class: unit]
      ~bin_response:[%bin_type_class: int list]
      ~bin_error:[%bin_type_class: unit]
      ()
  ;;

  let rpc_direct = basic_rpc "test-direct"
  let rpc_direct_big = big_rpc "test-direct-big"

  let make_direct rpc =
    let pipe_reader, pipe_writer = Pipe.create () in
    let impl =
      Rpc.Pipe_rpc.implement_direct rpc (fun () () writer ->
        Pipe.write_without_pushback pipe_writer writer;
        return (Ok ()))
    in
    ( impl
    , fun () ->
        match%map Pipe.read pipe_reader with
        | `Eof -> assert false
        | `Ok x -> x )
  ;;

  let implementation, next_direct_writer = make_direct rpc_direct
  let implementation_big, next_direct_writer_big = make_direct rpc_direct_big

  let implementations =
    Rpc.Implementations.create_exn
      ~implementations:[ implementation; implementation_big ]
      ~on_unknown_rpc:`Raise
      ~on_exception:Log_on_background_exn
  ;;

  let create fdr fdw =
    let reader = Rpc.Low_latency_transport.Reader.create fdr ~max_message_size ~config in
    let writer = Rpc.Low_latency_transport.Writer.create fdw ~max_message_size ~config in
    Async_rpc_kernel.Rpc.Connection.create
      ~implementations
      ~connection_state:ignore
      { reader; writer }
    >>| Result.ok_exn
  ;;

  let client, _server, server_to_client_fdw =
    Thread_safe.block_on_async_exn (fun () ->
      let%bind `Reader c2s_fdr, `Writer c2s_fdw =
        Unix.pipe (Info.of_string "client->server")
      in
      let%bind `Reader s2c_fdr, `Writer s2c_fdw =
        Unix.pipe (Info.of_string "server->client")
      in
      let%map client = create s2c_fdr c2s_fdw
      and server = create c2s_fdr s2c_fdw in
      client, server, s2c_fdw)
  ;;

  let new_direct_writer rpc next_direct_writer =
    Thread_safe.block_on_async_exn (fun () ->
      let%map () =
        match%map Rpc.Pipe_rpc.dispatch_iter rpc client () ~f:(fun _ -> Continue) with
        | Ok _ -> ()
        | Error _ -> assert false
      and direct_writer = next_direct_writer () in
      direct_writer)
  ;;

  let direct_writer = new_direct_writer rpc_direct next_direct_writer

  let direct_writers_big =
    Array.init 100 ~f:(fun _ -> new_direct_writer rpc_direct_big next_direct_writer_big)
  ;;

  let group1 = Rpc.Pipe_rpc.Direct_stream_writer.Group.create ()
  let group10 = Rpc.Pipe_rpc.Direct_stream_writer.Group.create ()
  let group100 = Rpc.Pipe_rpc.Direct_stream_writer.Group.create ()
  let groups = [ 1, group1; 10, group10; 100, group100 ]

  let () =
    List.iter groups ~f:(fun (n, group) ->
      for i = 0 to n - 1 do
        Rpc.Pipe_rpc.Direct_stream_writer.Group.add_exn group direct_writers_big.(i)
      done)
  ;;

  let () =
    (* Now replace the server fd by /dev/null, so that writes always succeed *)
    let fd = Fd.file_descr_exn server_to_client_fdw in
    let open! Core in
    let module Unix = Core_unix in
    let devnull = Unix.openfile ~mode:[ O_WRONLY ] "/dev/null" in
    Unix.dup2 ~src:devnull ~dst:fd ();
    Unix.close devnull
  ;;

  let data = Base.Sys.opaque_identity (42, 0x1234_5678)
  let bin_writer_int_int = [%bin_writer: int * int]
  let buf = Bigstring.create 8192

  let%bench "direct write" =
    Rpc.Pipe_rpc.Direct_stream_writer.write_without_pushback direct_writer data
    |> [%test_result: [ `Ok | `Closed ]] ~expect:`Ok
  ;;

  let%bench "direct write expert" =
    let len = bin_writer_int_int.write buf ~pos:0 data in
    Rpc.Pipe_rpc.Direct_stream_writer.Expert.write_without_pushback
      direct_writer
      ~buf
      ~pos:0
      ~len
    |> [%test_result: [ `Ok | `Closed ]] ~expect:`Ok
  ;;

  let big_data = Base.Sys.opaque_identity (List.init 1000 ~f:(fun x -> x * 1000))
  let bin_writer_int_list = [%bin_writer: int list]

  let%bench "direct write (big)" =
    Rpc.Pipe_rpc.Direct_stream_writer.write_without_pushback
      direct_writers_big.(0)
      big_data
    |> [%test_result: [ `Ok | `Closed ]] ~expect:`Ok
  ;;

  let%bench "direct write expert (big)" =
    let len = bin_writer_int_list.write buf ~pos:0 big_data in
    Rpc.Pipe_rpc.Direct_stream_writer.Expert.write_without_pushback
      direct_writers_big.(0)
      ~buf
      ~pos:0
      ~len
    |> [%test_result: [ `Ok | `Closed ]] ~expect:`Ok
  ;;

  let%bench ("iter direct write (big)" [@indexed n = [ 1; 10; 100 ]]) =
    for i = 0 to n - 1 do
      Rpc.Pipe_rpc.Direct_stream_writer.write_without_pushback
        direct_writers_big.(i)
        big_data
      |> [%test_result: [ `Ok | `Closed ]] ~expect:`Ok
    done
  ;;

  let%bench_fun ("direct write to group (big)" [@indexed n = [ 1; 10; 100 ]]) =
    let group = List.Assoc.find_exn groups ~equal:Int.equal n in
    fun () ->
      Rpc.Pipe_rpc.Direct_stream_writer.Group.write_without_pushback group big_data
  ;;

  let add_tracing_subscriber connection =
    let _subscriber =
      Bus.subscribe_exn
        (Async_rpc_kernel.Async_rpc_kernel_private.Connection.tracing_events connection)
        ~f:(fun event ->
          let (_ : _) = Base.Sys.opaque_identity event in
          ())
    in
    ()
  ;;

  let server_handle_connection
    ?(with_tracing_subscriber = false)
    transport
    implementations
    =
    match%bind
      Async_rpc_kernel.Rpc.Connection.create
        ~implementations
        ~connection_state:(fun (_ : Rpc.Connection.t) -> ())
        transport
    with
    | Ok connection ->
      if with_tracing_subscriber then add_tracing_subscriber connection;
      let%map () = Async_rpc_kernel.Rpc.Connection.close_finished connection in
      ()
    | Error _handshake_error ->
      let%map () = Async_rpc_kernel.Rpc.Transport.close transport in
      ()
  ;;

  let create_connection ?with_tracing_subscriber implementations =
    let client_end, server_end =
      Async_rpc_kernel.Pipe_transport.(create_pair Kind.string)
    in
    don't_wait_for
      (server_handle_connection server_end implementations ?with_tracing_subscriber);
    let%map client_conn =
      Async_rpc_kernel.Rpc.Connection.create
        ?implementations:None
        ~connection_state:(fun (_ : Rpc.Connection.t) -> ())
        client_end
      >>| Result.ok_exn
    in
    client_conn
  ;;

  let make_with_pipe rpc ~write =
    let impl =
      Rpc.Pipe_rpc.implement rpc (fun () () ->
        let pipe_reader, pipe_writer = Pipe.create () in
        write pipe_writer;
        Pipe.close pipe_writer;
        return (Ok pipe_reader))
    in
    impl
  ;;

  let rpc_pipe = basic_rpc "test-pipe"
  let rpc_pipe_big = big_rpc "test-pipe-big"

  let pipe_setup_conn ?(with_tracing_subscriber = false) rpc ~message_data ~num_messages =
    Thread_safe.block_on_async_exn (fun () ->
      let impl =
        make_with_pipe rpc ~write:(fun pipe_writer ->
          for _ = 1 to num_messages do
            Pipe.write_without_pushback pipe_writer message_data
          done)
      in
      let implementations =
        Rpc.Implementations.create_exn
          ~implementations:[ impl ]
          ~on_unknown_rpc:`Raise
          ~on_exception:Log_on_background_exn
      in
      let%map client_conn = create_connection implementations ~with_tracing_subscriber in
      if with_tracing_subscriber then add_tracing_subscriber client_conn;
      client_conn)
  ;;

  let read_messages
    (type a)
    (rpc : (unit, a, unit) Rpc.Pipe_rpc.t)
    connection
    ~num_messages
    =
    Thread_safe.block_on_async_exn (fun () ->
      let%bind reader, (_ : Rpc.Pipe_rpc.Metadata.t) =
        Rpc.Pipe_rpc.dispatch_exn rpc connection ()
      in
      let%map (_ : [ `Eof | `Exactly of a Queue.t | `Fewer of a Queue.t ]) =
        Pipe.read_exactly reader ~num_values:num_messages
      in
      ())
  ;;

  let%bench_fun ("end-to-end Pipe write (small)"
    [@indexed num_messages = [ 5; 500; 50_000 ]])
    =
    let client_conn = pipe_setup_conn rpc_pipe ~num_messages ~message_data:data in
    fun () -> read_messages rpc_pipe client_conn ~num_messages
  ;;

  let%bench_fun ("end-to-end Pipe write with tracing subscriber (small)"
    [@indexed num_messages = [ 5; 500; 50_000 ]])
    =
    let client_conn =
      pipe_setup_conn
        rpc_pipe
        ~with_tracing_subscriber:true
        ~num_messages
        ~message_data:data
    in
    fun () -> read_messages rpc_pipe client_conn ~num_messages
  ;;

  let%bench_fun ("end-to-end Pipe write (big)"
    [@indexed num_messages = [ 5; 500; 50_000 ]])
    =
    let client_conn = pipe_setup_conn rpc_pipe_big ~num_messages ~message_data:big_data in
    fun () -> read_messages rpc_pipe_big client_conn ~num_messages
  ;;

  let%bench_fun ("dispatch plain RPC" [@indexed num_dispatches = [ 1; 10; 100; 1_000 ]]) =
    let client_conn =
      Thread_safe.block_on_async_exn (fun () ->
        let implementations =
          Rpc.Implementations.create_exn
            ~implementations:
              [ Rpc.Rpc.implement plain_rpc (fun () () -> Deferred.never ()) ]
            ~on_unknown_rpc:`Raise
            ~on_exception:Log_on_background_exn
        in
        create_connection implementations)
    in
    fun () ->
      for _ = 1 to num_dispatches do
        ignore (Rpc.Rpc.dispatch plain_rpc client_conn () : int Or_error.t Deferred.t)
      done
  ;;
end

module%bench [@name "query metadata"] _ = struct
  module Protocol = Async_rpc_kernel.Async_rpc_kernel_private.Protocol

  let query_no_metadata : unit Protocol.Query.Validated.t =
    { tag = Protocol.Rpc_tag.of_string "tag"
    ; version = 0
    ; id = Protocol.Query_id.of_int_exn 1
    ; metadata = None
    ; data = ()
    }
  ;;

  let metadata_key_1, metadata_key_2 =
    Rpc.Rpc_metadata.V2.Key.of_int 0, Rpc.Rpc_metadata.V2.Key.of_int 1
  ;;

  let serialize query buf =
    Protocol.Message.bin_write_maybe_needs_length
      Bin_prot.Write.bin_write_unit
      buf
      ~pos:0
      query
  ;;

  let deserialize buf =
    Protocol.Message.bin_read_maybe_needs_length
      Bin_prot.Read.bin_read_unit
      buf
      ~pos_ref:(ref 0)
  ;;

  let deserialize__local buf = exclave_
    Async_rpc_kernel.Async_rpc_kernel_private.Protocol_local_readers.Message
    .bin_read_t__local
      Bin_prot.Read.bin_read_unit__local
      buf
      ~pos_ref:(ref 0)
  ;;

  let serialize_bench_helper query ~buf_len =
    let buf = Bigstring.create buf_len in
    fun () ->
      let bytes_written = serialize query buf in
      ignore (bytes_written : int);
      buf
  ;;

  let deserialize_bench_helper deserialize query ~buf_len =
    let buf = Bigstring.create buf_len in
    let (_ : int) = serialize query buf in
    fun () ->
      let query = deserialize buf in
      ignore query;
      buf
  ;;

  let bench_helper (payload_sizes_bytes, serialize_or_deserialize, metadata_type) =
    let bench_helper =
      match serialize_or_deserialize with
      | `Serialize -> serialize_bench_helper
      | `Deserialize -> deserialize_bench_helper deserialize
      | `Deserialize__local -> deserialize_bench_helper deserialize__local
    in
    let metadata =
      if payload_sizes_bytes = 0
      then None
      else
        Some
          (match metadata_type with
           | `V1 | `V2_single ->
             Rpc.Rpc_metadata.V2.of_alist
               [ ( metadata_key_1
                 , String.init payload_sizes_bytes ~f:(fun _ -> 'A')
                   |> Rpc.Rpc_metadata.V2.Payload.of_string_maybe_truncate )
               ]
             |> ok_exn
           | `V2_double ->
             Rpc.Rpc_metadata.V2.of_alist
               [ ( metadata_key_1
                 , String.init (payload_sizes_bytes / 2) ~f:(fun _ -> 'A')
                   |> Rpc.Rpc_metadata.V2.Payload.of_string_maybe_truncate )
               ; ( metadata_key_2
                 , String.init (payload_sizes_bytes / 2) ~f:(fun _ -> 'B')
                   |> Rpc.Rpc_metadata.V2.Payload.of_string_maybe_truncate )
               ]
             |> ok_exn)
    in
    let query = { query_no_metadata with metadata } in
    let query =
      match metadata_type with
      | `V1 -> Protocol.Query.Validated.to_v2 query |> Protocol.Message.Query_v2
      | `V2_single | `V2_double ->
        Protocol.Query.Validated.to_v3 query |> Protocol.Message.Query_v3
    in
    let buf_len =
      payload_sizes_bytes + 64
      (* 64 bytes should be enough for the rest of the binprotted query *)
    in
    bench_helper query ~buf_len
  ;;

  let payload_sizes_bytes =
    [ 2; 16; 128; 512; 1024 ] |> List.map ~f:(fun i -> Int.to_string i ^ " B payload", i)
  ;;

  let serialize_or_deserialize =
    [ "serialize", `Serialize
    ; "deserialize", `Deserialize
    ; "deserialize__local", `Deserialize__local
    ]
  ;;

  let metadata_type =
    [ "v1 metadata", `V1; "v2, 1 key", `V2_single; "v2, split across 2 keys", `V2_double ]
  ;;

  let bench_params
    ?(payload_sizes_bytes = payload_sizes_bytes)
    ?(serialize_or_deserialize = serialize_or_deserialize)
    ?(metadata_type = metadata_type)
    ()
    =
    List.cartesian_product
      payload_sizes_bytes
      (List.cartesian_product serialize_or_deserialize metadata_type)
    |> List.map
         ~f:
           (fun
             ( (payload_sizes_bytes_str, payload_sizes_bytes)
             , ((serialize_str, serialize), (metadata_str, metadata)) )
           ->
           ( String.concat
               [ payload_sizes_bytes_str; serialize_str; metadata_str ]
               ~sep:", "
           , (payload_sizes_bytes, serialize, metadata) ))
  ;;

  let%bench_fun ("no metadata"
    [@params
      params
      = bench_params
          ~payload_sizes_bytes:[ "None", 0 ]
          ~metadata_type:[ "v1 metadata", `V1; "v2, 1 key", `V2_single ]
          ()])
    =
    bench_helper params
  ;;

  let%bench_fun ("metadata exists" [@params params = bench_params ()]) =
    bench_helper params
  ;;
end
