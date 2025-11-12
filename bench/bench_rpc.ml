open Core
open Async

module type Config = sig
  val transport_name : string

  val create_connection_pair
    :  unit
    -> Bench_rpc_implementations.Connection_pair.t Deferred.t
end

let string_with_len len = String.init len ~f:(fun _ -> 'a') |> Sys.opaque_identity

module Make_benches (C : Config) = struct
  module%bench [@name_suffix sprintf "_%s_transport" C.transport_name] Rpc_benches =
  struct
    let%bench_fun "connection establishment" =
      fun () -> Thread_safe.block_on_async_exn C.create_connection_pair
    ;;

    let%bench_fun "connection establishment and closing" =
      fun () ->
      Thread_safe.block_on_async_exn (fun () ->
        let%bind { Bench_rpc_implementations.Connection_pair.server; client } =
          C.create_connection_pair ()
        in
        don't_wait_for (Rpc.Connection.close server);
        don't_wait_for (Rpc.Connection.close client);
        let%map () = Rpc.Connection.close_finished server
        and () = Rpc.Connection.close_finished client in
        ())
    ;;

    let%bench_fun ("one-way dispatch" [@indexed data_len = [ 1; 100; 10_000 ]]) =
      let { Bench_rpc_implementations.Connection_pair.client = conn; server = _ } =
        Thread_safe.block_on_async_exn C.create_connection_pair
      in
      let to_send = string_with_len data_len in
      fun () ->
        Rpc.One_way.dispatch_exn Bench_rpc_implementations.one_way_rpc conn to_send
    ;;

    let%bench_fun ("plain dispatch" [@indexed data_len = [ 1; 100; 10_000 ]]) =
      let { Bench_rpc_implementations.Connection_pair.client = conn; server = _ } =
        Thread_safe.block_on_async_exn C.create_connection_pair
      in
      let to_echo = string_with_len data_len in
      fun () ->
        ignore
          (Rpc.Rpc.dispatch Bench_rpc_implementations.echo_rpc conn to_echo
           : string Or_error.t Deferred.t)
    ;;

    let%bench_fun ("plain dispatch and response" [@indexed data_len = [ 1; 100; 10_000 ]])
      =
      let { Bench_rpc_implementations.Connection_pair.client = conn; server = _ } =
        Thread_safe.block_on_async_exn C.create_connection_pair
      in
      let to_echo = string_with_len data_len in
      fun () ->
        Thread_safe.block_on_async_exn (fun () ->
          let%map result =
            Rpc.Rpc.dispatch Bench_rpc_implementations.echo_rpc conn to_echo
          in
          ignore (result |> Or_error.ok_exn : string))
    ;;

    let%bench_fun ("plain dispatch and error response"
      [@indexed data_len = [ 1; 100; 10_000 ]])
      =
      let { Bench_rpc_implementations.Connection_pair.client = conn; server = _ } =
        Thread_safe.block_on_async_exn C.create_connection_pair
      in
      let error_data = Sexp.Atom (string_with_len data_len) in
      fun () ->
        Thread_safe.block_on_async_exn (fun () ->
          let%map result =
            Rpc.Rpc.dispatch Bench_rpc_implementations.error_rpc conn error_data
          in
          match result with
          | Error _ -> ()
          | Ok _ -> failwith "Expected error response")
    ;;

    let%bench_fun "pipe rpc initial dispatch" =
      let { Bench_rpc_implementations.Connection_pair.client = conn; server = _ } =
        Thread_safe.block_on_async_exn C.create_connection_pair
      in
      fun () ->
        ignore
          (Rpc.Pipe_rpc.dispatch Bench_rpc_implementations.pipe_rpc conn 0
           : (int Pipe.Reader.t * Rpc.Pipe_rpc.Metadata.t, unit) Result.t Or_error.t
               Deferred.t)
    ;;

    let%bench_fun "pipe rpc single update" =
      let { Bench_rpc_implementations.Connection_pair.client = conn; server = _ } =
        Thread_safe.block_on_async_exn C.create_connection_pair
      in
      fun () ->
        Thread_safe.block_on_async_exn (fun () ->
          match%bind Rpc.Pipe_rpc.dispatch Bench_rpc_implementations.pipe_rpc conn 1 with
          | Error _ -> failwith "Dispatch failed"
          | Ok (Error ()) -> failwith "RPC error"
          | Ok (Ok (reader, _metadata)) ->
            let%bind value = Pipe.read reader in
            (match value with
             | `Eof -> failwith "Should not get Eof"
             | `Ok _ -> ());
            Pipe.close_read reader;
            return ())
    ;;

    let%bench_fun ("pipe rpc full flow" [@indexed num_updates = [ 1; 10; 100 ]]) =
      let { Bench_rpc_implementations.Connection_pair.client = conn; server = _ } =
        Thread_safe.block_on_async_exn C.create_connection_pair
      in
      fun () ->
        Thread_safe.block_on_async_exn (fun () ->
          match%bind
            Rpc.Pipe_rpc.dispatch Bench_rpc_implementations.pipe_rpc conn num_updates
          with
          | Error _ -> failwith "Dispatch failed"
          | Ok (Error ()) -> failwith "RPC error"
          | Ok (Ok (reader, _metadata)) ->
            let%bind values = Pipe.to_list reader in
            assert (List.length values = num_updates);
            return ())
    ;;
  end
end
