open! Core
open! Async

let read_and_expect pipe ~expect =
  let%map value = Pipe.read_exn pipe in
  [%test_result: int] ~expect value
;;

let rpc =
  Rpc.Pipe_rpc.create
    ~name:"test"
    ~version:1
    ~bin_query:[%bin_type_class: unit]
    ~bin_response:[%bin_type_class: int]
    ~bin_error:[%bin_type_class: Nothing.t]
    ()
;;

let with_server_and_client group ~rpc ~f =
  let implementation =
    Rpc.Pipe_rpc.implement_direct rpc (fun () () writer ->
      Rpc.Pipe_rpc.Direct_stream_writer.Group.add_exn group writer;
      Deferred.Result.return ())
  in
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
  let%bind () =
    Rpc.Connection.with_client
      (Tcp.Where_to_connect.of_inet_address (Tcp.Server.listening_on_address server))
      (fun connection -> f server connection)
    >>| Or_error.of_exn_result
    >>| ok_exn
  in
  Tcp.Server.close server
;;

let%expect_test "[Direct_stream_writer.Group]: [send_last_value_on_add] sends values \
                 written with [write] and [Expert.write]"
  =
  let group =
    Rpc.Pipe_rpc.Direct_stream_writer.Group.create ~send_last_value_on_add:true ()
  in
  with_server_and_client group ~rpc ~f:(fun _ connection ->
    let%bind pipe1, (_ : Rpc.Pipe_rpc.Metadata.t) =
      Rpc.Pipe_rpc.dispatch_exn rpc connection ()
    in
    let%bind () = Rpc.Pipe_rpc.Direct_stream_writer.Group.write group 0 in
    let%bind () = read_and_expect pipe1 ~expect:0 in
    let%bind pipe2, (_ : Rpc.Pipe_rpc.Metadata.t) =
      Rpc.Pipe_rpc.dispatch_exn rpc connection ()
    in
    let%bind () = read_and_expect pipe2 ~expect:0 in
    let value_to_write = 42 in
    let value_size = [%bin_size: int] value_to_write in
    let buf = Bigstring.create value_size in
    let (_ : int) = [%bin_write: int] buf ~pos:0 value_to_write in
    let%bind () =
      Rpc.Pipe_rpc.Direct_stream_writer.Group.Expert.write
        group
        ~buf
        ~pos:0
        ~len:value_size
    in
    let%bind () = read_and_expect pipe1 ~expect:value_to_write in
    let%bind () = read_and_expect pipe2 ~expect:value_to_write in
    let%bind pipe3, (_ : Rpc.Pipe_rpc.Metadata.t) =
      Rpc.Pipe_rpc.dispatch_exn rpc connection ()
    in
    let%bind () = read_and_expect pipe3 ~expect:value_to_write in
    Deferred.unit)
;;

let%expect_test "[Direct_stream_writer.Group]: [send_last_value_on_add] will save values \
                 written when there are no writers"
  =
  let group =
    Rpc.Pipe_rpc.Direct_stream_writer.Group.create ~send_last_value_on_add:true ()
  in
  let last_value = ref 0 in
  let write_and_record_last_value value =
    let%map () = Rpc.Pipe_rpc.Direct_stream_writer.Group.write group value in
    last_value := value
  in
  with_server_and_client group ~rpc ~f:(fun _ connection ->
    let%bind () = write_and_record_last_value 0 in
    let%bind pipe1, (_ : Rpc.Pipe_rpc.Metadata.t) =
      Rpc.Pipe_rpc.dispatch_exn rpc connection ()
    in
    let%bind () = read_and_expect pipe1 ~expect:!last_value in
    let%bind () = write_and_record_last_value 1 in
    let%bind () = read_and_expect pipe1 ~expect:!last_value in
    let () = Pipe.close_read pipe1 in
    let%bind () = Pipe.closed pipe1 in
    let%bind () =
      Deferred.repeat_until_finished () (fun () ->
        match Rpc.Pipe_rpc.Direct_stream_writer.Group.length group with
        | 0 -> return (`Finished ())
        | _ ->
          let%map () = Scheduler.yield_until_no_jobs_remain () in
          `Repeat ())
    in
    let%bind () = write_and_record_last_value 2 in
    let%bind pipe2, (_ : Rpc.Pipe_rpc.Metadata.t) =
      Rpc.Pipe_rpc.dispatch_exn rpc connection ()
    in
    let%bind () = read_and_expect pipe2 ~expect:!last_value in
    let%bind () = write_and_record_last_value 3 in
    let%bind () = read_and_expect pipe2 ~expect:!last_value in
    return ())
;;

let%expect_test "[Direct_stream_writer.Group]: [send_last_value_on_add] works with \
                 0-length values"
  =
  let rpc =
    Rpc.Pipe_rpc.create
      ~name:"test"
      ~version:1
      ~bin_query:[%bin_type_class: unit]
      ~bin_response:[%bin_type_class: Unit.Stable.V2.t]
      ~bin_error:[%bin_type_class: Nothing.t]
      ()
  in
  let group =
    Rpc.Pipe_rpc.Direct_stream_writer.Group.create ~send_last_value_on_add:true ()
  in
  with_server_and_client group ~rpc ~f:(fun _ connection ->
    let%bind pipe1, (_ : Rpc.Pipe_rpc.Metadata.t) =
      Rpc.Pipe_rpc.dispatch_exn rpc connection ()
    in
    let%bind () = Rpc.Pipe_rpc.Direct_stream_writer.Group.write group () in
    let%bind () = Pipe.read_exn pipe1 in
    let%bind pipe2, (_ : Rpc.Pipe_rpc.Metadata.t) =
      Rpc.Pipe_rpc.dispatch_exn rpc connection ()
    in
    let%bind () = Pipe.read_exn pipe2 in
    Deferred.unit)
;;
