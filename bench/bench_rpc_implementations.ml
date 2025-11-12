open Core
open Async

let one_way_rpc =
  Rpc.One_way.create ~name:"bench-one-way" ~version:1 ~bin_msg:[%bin_type_class: string]
;;

let echo_rpc =
  Rpc.Rpc.create
    ~name:"bench-echo"
    ~version:1
    ~bin_query:[%bin_type_class: string]
    ~bin_response:[%bin_type_class: string]
    ~include_in_error_count:Only_on_exn
;;

let error_rpc =
  Rpc.Rpc.create
    ~name:"bench-error"
    ~version:1
    ~bin_query:[%bin_type_class: Sexp.t]
    ~bin_response:[%bin_type_class: unit]
    ~include_in_error_count:Only_on_exn
;;

let pipe_rpc =
  Rpc.Pipe_rpc.create
    ~name:"bench-pipe"
    ~version:1
    ~bin_query:[%bin_type_class: int]
    ~bin_response:[%bin_type_class: int]
    ~bin_error:[%bin_type_class: unit]
    ()
;;

let implementations =
  [ Rpc.One_way.implement
      one_way_rpc
      (fun () _msg -> ())
      ~on_exception:(Raise_to_monitor Monitor.main)
  ; Rpc.Rpc.implement echo_rpc (fun () msg -> return msg)
  ; Rpc.Rpc.implement error_rpc (fun () sexp ->
      raise_s [%message "Intentional error" (sexp : Sexp.t)])
  ; Rpc.Pipe_rpc.implement pipe_rpc (fun () count ->
      let reader =
        Pipe.create_reader ~close_on_exception:false (fun writer ->
          let rec loop i =
            if i > count
            then return ()
            else (
              let%bind () = Pipe.write writer i in
              loop (i + 1))
          in
          loop 1)
      in
      return (Ok reader))
  ]
;;

module Connection_pair = struct
  type t =
    { server : Rpc.Connection.t
    ; client : Rpc.Connection.t
    }
end

let connection_pair_of_transports ~client_transport ~server_transport =
  let%map client_conn =
    Async_rpc_kernel.Rpc.Connection.create
      ~connection_state:(fun _ -> ())
      client_transport
    >>| Result.ok_exn
  and server_conn =
    Async_rpc_kernel.Rpc.Connection.create
      ~implementations:
        (Rpc.Implementations.create_exn
           ~implementations
           ~on_unknown_rpc:`Raise
           ~on_exception:Close_connection)
      ~connection_state:(fun _ -> ())
      server_transport
    >>| Result.ok_exn
  in
  { Connection_pair.server = server_conn; client = client_conn }
;;

let create_connection_pair_with_pipe_transport () =
  let client_transport, server_transport =
    Async_rpc_kernel.Pipe_transport.(create_pair Kind.string)
  in
  connection_pair_of_transports ~client_transport ~server_transport
;;

let create_connection_pair_with_async_transport () =
  let max_message_size = 16 lsl 20 in
  let%bind `Reader client_reader_fd, `Writer server_writer_fd =
    Unix.pipe (Info.of_string "client->server")
  in
  let%bind `Reader server_reader_fd, `Writer client_writer_fd =
    Unix.pipe (Info.of_string "server->client")
  in
  let client_reader = Reader.create client_reader_fd in
  let client_writer = Writer.create client_writer_fd in
  let server_reader = Reader.create server_reader_fd in
  let server_writer = Writer.create server_writer_fd in
  let client_transport =
    Rpc.Transport.of_reader_writer ~max_message_size client_reader client_writer
  in
  let server_transport =
    Rpc.Transport.of_reader_writer ~max_message_size server_reader server_writer
  in
  connection_pair_of_transports ~client_transport ~server_transport
;;
