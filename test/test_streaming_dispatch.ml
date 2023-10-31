open! Core
open! Async

let pipe_rpc =
  Rpc.Pipe_rpc.create
    ~name:"pipe"
    ~version:1
    ~bin_query:[%bin_type_class: unit]
    ~bin_response:[%bin_type_class: unit]
    ~bin_error:[%bin_type_class: string]
    ()
;;

let state_rpc =
  Rpc.State_rpc.create
    ~name:"state"
    ~version:1
    ~bin_query:[%bin_type_class: unit]
    ~bin_state:[%bin_type_class: unit]
    ~bin_update:[%bin_type_class: unit]
    ~bin_error:[%bin_type_class: string]
    ()
;;

let with_server_and_conn implementation ~f : unit Deferred.t =
  let implementations =
    Rpc.Implementations.create_exn
      ~implementations:[ implementation ]
      ~on_unknown_rpc:`Raise
  in
  let%bind server =
    Rpc.Connection.serve
      ~implementations
      ~initial_connection_state:(fun (_ : [< Socket.Address.t ]) (_ : Rpc.Connection.t) ->
        ())
      ~where_to_listen:Tcp.Where_to_listen.of_port_chosen_by_os
      ()
  in
  Rpc.Connection.with_client
    (Tcp.Where_to_connect.of_inet_address (Tcp.Server.listening_on_address server))
    f
  >>| Result.ok_exn
;;

(* This works for both the (_,string) result Or_error.t types we get from dispatch and the
   (_,string) result Rpc_result.t types we get from dispatch' *)
let unwrap_results = function
  | Ok (Ok v) -> v
  | Ok (Error (_ : string)) -> assert false
  | Error _ -> assert false
;;

let write_pipe_close_and_drain writer_ref get_pipe_reader_from_rpc_result result =
  let%bind result = result in
  let reader = get_pipe_reader_from_rpc_result (unwrap_results result) in
  let writer = Option.value_exn !writer_ref in
  let%bind () = Pipe.write writer () in
  Pipe.close writer;
  Pipe.to_list reader
;;

let%expect_test "[Pipe_rpc.dispatch] and [Pipe_rpc.dispatch'] don’t wait for the pipe to \
                 close before being determined"
  =
  let writer_ref = ref None in
  with_server_and_conn
    (Rpc.Pipe_rpc.implement pipe_rpc (fun () () ->
       let reader, writer = Pipe.create () in
       writer_ref := Some writer;
       Deferred.Result.return reader))
    ~f:(fun connection ->
      let%bind () =
        let%map result =
          Rpc.Pipe_rpc.dispatch pipe_rpc connection ()
          |> write_pipe_close_and_drain writer_ref Tuple2.get1
        in
        print_s [%sexp (result : unit list)];
        [%expect {| (()) |}]
      in
      let%bind () =
        let%map result =
          Rpc.Pipe_rpc.dispatch' pipe_rpc connection ()
          |> write_pipe_close_and_drain writer_ref Tuple2.get1
        in
        print_s [%sexp (result : unit list)];
        [%expect {| (()) |}]
      in
      Deferred.unit)
;;

let%expect_test "[State_rpc.dispatch] and [State_rpc.dispatch'] don’t wait for the pipe \
                 to close before being determined"
  =
  let writer_ref = ref None in
  with_server_and_conn
    (Rpc.State_rpc.implement state_rpc (fun () () ->
       let reader, writer = Pipe.create () in
       writer_ref := Some writer;
       Deferred.Result.return ((), reader)))
    ~f:(fun connection ->
      let%bind () =
        let%map result =
          Rpc.State_rpc.dispatch state_rpc connection ()
          |> write_pipe_close_and_drain writer_ref Tuple3.get2
        in
        print_s [%sexp (result : unit list)];
        [%expect {| (()) |}]
      in
      let%bind () =
        let%map result =
          Rpc.State_rpc.dispatch' state_rpc connection ()
          |> write_pipe_close_and_drain writer_ref Tuple3.get2
        in
        print_s [%sexp (result : unit list)];
        [%expect {| (()) |}]
      in
      Deferred.unit)
;;
