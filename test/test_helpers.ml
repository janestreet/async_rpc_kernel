open! Core
open! Async

let rpc_tag = "test-rpc"
let rpc_version = 1

let rpc =
  Rpc.Rpc.create
    ~name:rpc_tag
    ~version:rpc_version
    ~bin_query:Bigstring.Stable.V1.bin_t
    ~bin_response:Bigstring.Stable.V1.bin_t
;;

let one_way_rpc =
  Rpc.One_way.create
    ~name:"test-one-way-rpc"
    ~version:1
    ~bin_msg:Bigstring.Stable.V1.bin_t
;;

let pipe_rpc =
  Rpc.Pipe_rpc.create
    ~name:"test-pipe-rpc"
    ~version:1
    ~bin_query:Bigstring.Stable.V1.bin_t
    ~bin_response:Bigstring.Stable.V1.bin_t
    ~bin_error:Error.Stable.V2.bin_t
    ()
;;

let state_rpc =
  Rpc.State_rpc.create
    ~name:"test-state-rpc"
    ~version:1
    ~bin_query:Bigstring.Stable.V1.bin_t
    ~bin_state:Bigstring.Stable.V1.bin_t
    ~bin_update:Bigstring.Stable.V1.bin_t
    ~bin_error:Error.Stable.V2.bin_t
    ()
;;

module Header = Async_rpc_kernel.Async_rpc_kernel_private.Connection.For_testing.Header

(* Helpers for manipulating handshake types *)
open struct
  let with_handshake_header header =
    Async_rpc_kernel.Async_rpc_kernel_private.Connection.For_testing
    .with_async_execution_context
      ~context:header
  ;;
end

let implementations =
  [ Rpc.Rpc.implement rpc (fun () payload -> return payload)
  ; Rpc.One_way.implement one_way_rpc (fun () _message -> ())
  ; Rpc.Pipe_rpc.implement pipe_rpc (fun () _query ->
      Deferred.Or_error.return
        (Pipe.create_reader ~close_on_exception:true (fun writer ->
           let response = Bigstring.of_string "response" in
           let%bind () = Pipe.write writer response in
           let%bind () = Pipe.write writer response in
           Pipe.close writer;
           return ())))
  ; Rpc.State_rpc.implement state_rpc (fun () _query ->
      let state = Bigstring.of_string "state" in
      let reader =
        Pipe.create_reader ~close_on_exception:true (fun writer ->
          let update = Bigstring.of_string "update" in
          let%bind () = Pipe.write writer update in
          let%bind () = Pipe.write writer update in
          Pipe.close writer;
          return ())
      in
      Deferred.Or_error.return (state, reader))
  ]
;;

let with_local_connection ?(lift_implementation = Fn.id) ~header ~f () =
  let%bind `Reader reader_fd, `Writer writer_fd =
    Unix.pipe (Info.of_string "rpc_test 1")
  in
  let reader = Reader.create reader_fd in
  let writer = Writer.create writer_fd in
  let implementations =
    let implementations = List.map implementations ~f:lift_implementation in
    Rpc.Implementations.create_exn ~implementations ~on_unknown_rpc:`Raise
  in
  let%bind conn =
    with_handshake_header header ~f:(fun () ->
      Rpc.Connection.create ~implementations ~connection_state:(const ()) reader writer
      >>| Result.ok_exn)
  in
  let%bind result = f conn in
  let%bind () = Rpc.Connection.close conn in
  return result
;;

let only_heartbeat_once_at_the_beginning =
  Rpc.Connection.Heartbeat_config.create
    ~timeout:(Time_ns.Span.of_sec 360.)
    ~send_every:(Time_ns.Span.of_sec 120.)
    ()
;;

let with_rpc_server_connection ~server_header ~client_header ~f =
  let receiver_ivar = Ivar.create () in
  let%bind server =
    with_handshake_header server_header ~f:(fun () ->
      Rpc.Connection.serve
        ~heartbeat_config:only_heartbeat_once_at_the_beginning
        ~implementations:
          (Rpc.Implementations.create_exn ~implementations ~on_unknown_rpc:`Raise)
        ~initial_connection_state:(fun _ conn ->
          Ivar.fill_exn receiver_ivar conn;
          ())
        ~where_to_listen:Tcp.Where_to_listen.of_port_chosen_by_os
        ())
  in
  let port = Tcp.Server.listening_on server in
  let where_to_connect =
    Tcp.Where_to_connect.of_host_and_port { Host_and_port.host = "localhost"; port }
  in
  let%bind sender =
    with_handshake_header client_header ~f:(fun () ->
      Rpc.Connection.client
        ~heartbeat_config:only_heartbeat_once_at_the_beginning
        where_to_connect
      >>| Result.ok_exn)
  in
  let%bind receiver = Ivar.read receiver_ivar in
  let%bind result = f ~sender ~receiver in
  let%bind () = Rpc.Connection.close sender in
  let%bind () = Tcp.Server.close server in
  return result
;;
