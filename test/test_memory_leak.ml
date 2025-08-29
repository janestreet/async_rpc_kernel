open! Core
open! Async

let rpc =
  Rpc.Rpc.create
    ~name:"memory-test-rpc"
    ~version:1
    ~bin_query:String.bin_t
    ~bin_response:String.bin_t
    ~include_in_error_count:Only_on_exn
;;

let implementations = [ Rpc.Rpc.implement rpc (fun () query -> return query) ]

(* We avoid using [Test_helpers.with_rpc_server_connection] because the "taps" it installs
   for snooping on communication between the client and the server can't handle tens of
   thousands of RPCs, and we actually don't want to buffer any additional state anyway. *)
let with_connection ~f =
  let server_ivar = Ivar.create () in
  let%bind server =
    Rpc.Connection.serve
      ~implementations:
        (Rpc.Implementations.create_exn
           ~implementations
           ~on_unknown_rpc:`Raise
           ~on_exception:Log_on_background_exn)
      ~initial_connection_state:(fun _ conn ->
        Ivar.fill_exn server_ivar conn;
        ())
      ~where_to_listen:Tcp.Where_to_listen.of_port_chosen_by_os
      ()
  in
  let port = Tcp.Server.listening_on server in
  let where_to_connect =
    Tcp.Where_to_connect.of_host_and_port { Host_and_port.host = "localhost"; port }
  in
  let%bind client = Rpc.Connection.client where_to_connect >>| Result.ok_exn in
  let%bind server_conn = Ivar.read server_ivar in
  let%bind result = f ~client ~server:server_conn in
  let%bind () = Rpc.Connection.close client in
  let%bind () = Tcp.Server.close server in
  return result
;;

let get_heap_words () =
  let stats = Gc.stat () in
  stats.heap_words |> Byte_units.of_words_int
;;

let%expect_test "regression test for memory leak per RPC dispatch" =
  let num_rpcs = 1000 in
  with_connection ~f:(fun ~client ~server:_ ->
    Gc.full_major ();
    let before_heap = get_heap_words () in
    let send_rpc i =
      let query = sprintf "query_%d" i in
      (* We want to ensure that we don't leak memory per RPC dispatch. *)
      let%bind response = Rpc.Rpc.dispatch rpc client query >>| ok_exn in
      [%test_result: string] response ~expect:query;
      (* We additionally want to ensure that peeking at the close reason does not leak
             memory. *)
      let close_started =
        Rpc.Connection.close_reason ~on_close:`started client
        |> Deferred.peek
        |> Option.is_some
      in
      assert (not close_started);
      return ()
    in
    let%bind () = Deferred.for_ 0 ~to_:num_rpcs ~do_:send_rpc in
    Gc.full_major ();
    let after_heap = get_heap_words () in
    let heap_growth = Byte_units.(after_heap - before_heap) in
    [%test_eq: Byte_units.t] heap_growth Byte_units.zero;
    return ())
;;
