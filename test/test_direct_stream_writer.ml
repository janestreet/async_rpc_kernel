open! Core
open! Async

let read_and_print pipe =
  let%map value = Pipe.read_exn pipe in
  print_s [%sexp (value : int)]
;;

let%expect_test "[Direct_stream_writer.Group]: [send_last_value_on_add] sends values \
                 written with [write] and [Expert.write]"
  =
  let rpc =
    Rpc.Pipe_rpc.create
      ~name:"test"
      ~version:1
      ~bin_query:[%bin_type_class: unit]
      ~bin_response:[%bin_type_class: int]
      ~bin_error:[%bin_type_class: Nothing.t]
      ()
  in
  let group =
    Rpc.Pipe_rpc.Direct_stream_writer.Group.create ~send_last_value_on_add:true ()
  in
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
      (fun connection ->
         let%bind pipe1, (_ : Rpc.Pipe_rpc.Metadata.t) =
           Rpc.Pipe_rpc.dispatch_exn rpc connection ()
         in
         let%bind () = Rpc.Pipe_rpc.Direct_stream_writer.Group.write group 0 in
         let%bind () = read_and_print pipe1 in
         [%expect {| 0 |}];
         let%bind pipe2, (_ : Rpc.Pipe_rpc.Metadata.t) =
           Rpc.Pipe_rpc.dispatch_exn rpc connection ()
         in
         let%bind () = read_and_print pipe2 in
         [%expect {| 0 |}];
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
         let%bind () = read_and_print pipe1 in
         [%expect {| 42 |}];
         let%bind () = read_and_print pipe2 in
         [%expect {| 42 |}];
         let%bind pipe3, (_ : Rpc.Pipe_rpc.Metadata.t) =
           Rpc.Pipe_rpc.dispatch_exn rpc connection ()
         in
         let%bind () = read_and_print pipe3 in
         [%expect {| 42 |}];
         Deferred.unit)
    >>| Or_error.of_exn_result
    >>| ok_exn
  in
  Tcp.Server.close server
;;
