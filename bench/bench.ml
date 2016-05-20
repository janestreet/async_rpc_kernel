open Core.Std
open Async.Std

let max_message_size = 16 lsl 20

let config =
  Rpc.Low_latency_transport.Config.create
    (* Always batch, we don't care about measuring the syscall time here *)
    ~start_batching_after_num_messages:0
    ()

let rpc =
  Rpc.Pipe_rpc.create
    ~name:"test"
    ~version:42
    ~bin_query:[%bin_type_class: unit]
    ~bin_response:[%bin_type_class: int * int]
    ~bin_error:[%bin_type_class: unit]
    ()

let implementation, direct_writer =
  let ivar = Ivar.create () in
  let impl =
    Rpc.Pipe_rpc.implement_direct rpc
      (fun () () writer ->
         Ivar.fill ivar writer;
         return (Ok ()))
  in
  (impl, Ivar.read ivar)

let implementations =
  Rpc.Implementations.create_exn
    ~implementations:[implementation]
    ~on_unknown_rpc:`Raise

let create fdr fdw =
  let reader = Rpc.Low_latency_transport.Reader.create fdr ~max_message_size ~config in
  let writer = Rpc.Low_latency_transport.Writer.create fdw ~max_message_size ~config in
  Async_rpc_kernel.Std.Rpc.Connection.create
    ~implementations ~connection_state:ignore
    { reader; writer }
  >>| Result.ok_exn

let client, server, server_to_client_fdw =
  Thread_safe.block_on_async_exn (fun () ->
    let%bind (`Reader c2s_fdr, `Writer c2s_fdw) =
      Unix.pipe (Info.of_string "client->server")
    in
    let%bind (`Reader s2c_fdr, `Writer s2c_fdw) =
      Unix.pipe (Info.of_string "server->client")
    in
    let%map client = create s2c_fdr c2s_fdw
    and     server = create c2s_fdr s2c_fdw
    in
    (client, server, s2c_fdw))

let direct_writer =
  Thread_safe.block_on_async_exn (fun () ->
    let%map () =
      match%map Rpc.Pipe_rpc.dispatch_iter rpc client () ~f:(fun _ -> Continue) with
      | Ok _    -> ()
      | Error _ -> assert false
    and direct_writer = direct_writer in
    direct_writer)

external stop_inlining : 'a -> 'a = "async_rpc_kernel_bench_identity"

let data = stop_inlining (42, 0x1234_5678)

let () =
  (* Now replace the server fd by /dev/null, so that writes always succeed *)
  let fd = Fd.file_descr_exn server_to_client_fdw in
  let open Core.Std in
  let devnull = Unix.openfile ~mode:[O_WRONLY] "/dev/null" in
  Unix.dup2 ~src:devnull ~dst:fd;
  Unix.close devnull
;;

let%bench "direct write" =
  Rpc.Pipe_rpc.Direct_stream_writer.write_without_pushback direct_writer
    data
;;

let buf = Bigstring.create 32
let len = [%bin_writer: int * int].write buf ~pos:0 data

let%bench "direct write expert" =
  Rpc.Pipe_rpc.Direct_stream_writer.Expert.write_without_pushback direct_writer
    ~buf ~pos:0 ~len
;;
