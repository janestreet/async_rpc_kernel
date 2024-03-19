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

let rpc_with_pushback =
  Rpc.Pipe_rpc.create
    ~name:"test-with-pushback"
    ~version:1
    ~bin_query:[%bin_type_class: unit]
    ~bin_response:[%bin_type_class: Bigstring.Stable.V1.t]
    ~bin_error:[%bin_type_class: Nothing.t]
    ~client_pushes_back:()
    ()
;;

let with_server_and_client' ~rpc ~f ~on_writer =
  let implementation =
    Rpc.Pipe_rpc.implement_direct rpc (fun () () writer ->
      on_writer writer;
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

let with_server_and_client group ~rpc ~f =
  with_server_and_client' ~rpc ~f ~on_writer:(fun writer ->
    Rpc.Pipe_rpc.Direct_stream_writer.Group.add_exn group writer)
;;

let%expect_test "[Direct_stream_writer.Expert.schedule_write] never becomes determined \
                 if the connection is closed"
  =
  let saved_writer = ref None in
  let scheduled_response = Ivar.create () in
  with_server_and_client'
    ~rpc:rpc_with_pushback
    ~on_writer:(fun writer ->
      saved_writer := Some writer;
      (* We want a big enough response that just getting the pipe (and not reading any
         elements) will leave us with bytes in the sendq. So we make a big bigstring,
         schedule it 100 times, and keep the last deferred for when we can reuse the
         bigstring *)
      let response = Bigstring.create 1_000_000 in
      let buf =
        Bin_prot.Writer.to_bigstring [%bin_writer: Bigstring.Stable.V1.t] response
      in
      List.init 100 ~f:ignore
      |> List.fold ~init:None ~f:(fun last_scheduled_response () ->
           match
             Rpc.Pipe_rpc.Direct_stream_writer.Expert.schedule_write
               writer
               ~buf
               ~pos:0
               ~len:(Bigstring.length buf)
           with
           | `Closed -> last_scheduled_response
           | `Flushed { g = d } -> Some d)
      |> Ivar.fill_exn scheduled_response)
    ~f:(fun (_ : (Socket.Address.Inet.t, int) Tcp.Server.t) connection ->
      let%bind (_ : Bigstring.t Pipe.Reader.t), metadata =
        Rpc.Pipe_rpc.dispatch_exn rpc_with_pushback connection ()
      in
      let%bind () = Rpc.Connection.close connection in
      let writer = !saved_writer |> Option.value_exn in
      let%bind () = Rpc.Pipe_rpc.Direct_stream_writer.closed writer in
      let%bind scheduled_response = Ivar.read scheduled_response in
      print_s ([%sexp_of: unit Deferred.t option] scheduled_response);
      [%expect {| (Empty) |}];
      let%bind reason = Rpc.Pipe_rpc.close_reason metadata in
      print_s ([%sexp_of: Rpc.Pipe_close_reason.t] reason);
      [%expect {| (Error (Connection_closed (Rpc.Connection.close))) |}];
      return ())
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
