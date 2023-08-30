open Core
open! Poly
open Async

let max_message_size = 16 lsl 20

let config =
  Rpc.Low_latency_transport.Config.create
  (* Always batch, we don't care about measuring the syscall time here *)
    ~start_batching_after_num_messages:0
    ()
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
  assert (
    Rpc.Pipe_rpc.Direct_stream_writer.write_without_pushback direct_writer data = `Ok)
;;

let%bench "direct write expert" =
  let len = bin_writer_int_int.write buf ~pos:0 data in
  assert (
    Rpc.Pipe_rpc.Direct_stream_writer.Expert.write_without_pushback
      direct_writer
      ~buf
      ~pos:0
      ~len
    = `Ok)
;;

let big_data = Base.Sys.opaque_identity (List.init 1000 ~f:(fun x -> x * 1000))
let bin_writer_int_list = [%bin_writer: int list]

let%bench "direct write (big)" =
  assert (
    Rpc.Pipe_rpc.Direct_stream_writer.write_without_pushback
      direct_writers_big.(0)
      big_data
    = `Ok)
;;

let%bench "direct write expert (big)" =
  let len = bin_writer_int_list.write buf ~pos:0 big_data in
  assert (
    Rpc.Pipe_rpc.Direct_stream_writer.Expert.write_without_pushback
      direct_writers_big.(0)
      ~buf
      ~pos:0
      ~len
    = `Ok)
;;

let%bench ("iter direct write (big)" [@indexed n = [ 1; 10; 100 ]]) =
  for i = 0 to n - 1 do
    assert (
      Rpc.Pipe_rpc.Direct_stream_writer.write_without_pushback
        direct_writers_big.(i)
        big_data
      = `Ok)
  done
;;

let%bench_fun ("direct write to group (big)" [@indexed n = [ 1; 10; 100 ]]) =
  let group = List.Assoc.find_exn groups ~equal:Int.equal n in
  fun () -> Rpc.Pipe_rpc.Direct_stream_writer.Group.write_without_pushback group big_data
;;

let add_tracing_subscriber connection =
  let _subscriber =
    Bus.subscribe_exn
      (Async_rpc_kernel.Async_rpc_kernel_private.Connection.events connection)
      [%here]
      ~f:(fun event ->
      let (_ : _) = Base.Sys.opaque_identity event in
      ())
  in
  ()
;;

let server_handle_connection ?(with_tracing_subscriber = false) transport implementations =
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
  let to_server_reader, to_server_writer = Pipe.create () in
  let to_client_reader, to_client_writer = Pipe.create () in
  let one_transport =
    Async_rpc_kernel.Pipe_transport.create Async_rpc_kernel.Pipe_transport.Kind.string
  in
  let client_end = one_transport to_client_reader to_server_writer in
  let server_end = one_transport to_server_reader to_client_writer in
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
      Rpc.Implementations.create_exn ~implementations:[ impl ] ~on_unknown_rpc:`Raise
    in
    let%map client_conn = create_connection implementations ~with_tracing_subscriber in
    if with_tracing_subscriber then add_tracing_subscriber client_conn;
    client_conn)
;;

let read_messages (type a) (rpc : (unit, a, unit) Rpc.Pipe_rpc.t) connection ~num_messages
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

let%bench_fun ("end-to-end Pipe write (small)" [@indexed
                                                 num_messages = [ 5; 500; 50_000 ]])
  =
  let client_conn = pipe_setup_conn rpc_pipe ~num_messages ~message_data:data in
  fun () -> read_messages rpc_pipe client_conn ~num_messages
;;

let%bench_fun ("end-to-end Pipe write with tracing subscriber (small)" [@indexed
                                                                         num_messages
                                                                         = [ 5
                                                                           ; 500
                                                                           ; 50_000
                                                                           ]])
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

let%bench_fun ("end-to-end Pipe write (big)" [@indexed num_messages = [ 5; 500; 50_000 ]])
  =
  let client_conn = pipe_setup_conn rpc_pipe_big ~num_messages ~message_data:big_data in
  fun () -> read_messages rpc_pipe_big client_conn ~num_messages
;;
