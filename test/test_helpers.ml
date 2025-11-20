open! Core
open! Async

let rpc =
  Rpc.Rpc.create
    ~name:"test-rpc"
    ~version:1
    ~bin_query:Bigstring.Stable.V1.bin_t
    ~bin_response:Bigstring.Stable.V1.bin_t
    ~include_in_error_count:Only_on_exn
;;

let rpc_v2 =
  Rpc.Rpc.create
    ~name:"test-rpc"
    ~version:2
    ~bin_query:Bigstring.Stable.V1.bin_t
    ~bin_response:Bigstring.Stable.V1.bin_t
    ~include_in_error_count:Only_on_exn
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

let sort_rpc =
  Rpc.Rpc.create
    ~name:"sort"
    ~version:1
    ~bin_query:[%bin_type_class: int array]
    ~bin_response:[%bin_type_class: int array]
    ~include_in_error_count:Only_on_exn
;;

(* Short ids to reduce size of test output *)
let server_identification = Bigstring.of_string "serv-id"
let client_identification = Bigstring.of_string "clin-id"

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
  ; Rpc.One_way.implement
      one_way_rpc
      (fun () _message -> ())
      ~on_exception:Close_connection
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
  ; Rpc.Rpc.implement sort_rpc (fun () payload ->
      Array.sort payload ~compare;
      return payload)
  ; Rpc.Rpc.implement rpc_v2 (fun () payload -> return payload)
  ]
;;

module Tap = struct
  type t = (Bigstring.t -> pos:int -> len:int -> unit) -> unit

  let create () =
    (* Should be big enough for tests *)
    let buf = Bigstring.create 10_000 in
    let pos = ref 0 in
    let tap f =
      f buf ~pos:0 ~len:!pos;
      pos := 0
    in
    let record_chunk ~src ~src_pos ~len =
      Bigstring.blit ~src ~dst:buf ~src_pos ~dst_pos:!pos ~len;
      pos := !pos + len
    in
    tap, record_chunk
  ;;

  let print_header t =
    t (fun buf ~pos ~len ->
      (* This is mostly mirrored off of {!print_messages}, just unrolled to only print the
         handshake header and connection metadata (if it exists) *)
      let header_len =
        Async_rpc_kernel.Async_rpc_kernel_private.Transport.Header.length
        + Bigstring.get_int64_le_exn buf ~pos
      in
      Binio_printer_helper.parse_and_print
        [ [%bin_shape:
            Async_rpc_kernel.Async_rpc_kernel_private.Connection.For_testing.Header.t
              Binio_printer_helper.With_length64.t]
        ]
        (Bigstring.sub_shared ~pos ~len:header_len buf)
        ~pos:0;
      if header_len <> len
      then (
        let metadata_pos = pos + header_len in
        let metadata_len =
          Async_rpc_kernel.Async_rpc_kernel_private.Transport.Header.length
          + Bigstring.get_int64_le_exn buf ~pos:metadata_pos
        in
        print_endline "";
        Binio_printer_helper.parse_and_print
          [ [%bin_shape:
              Nothing.t Binio_printer_helper.With_length.t
                Async_rpc_kernel.Async_rpc_kernel_private.Protocol.Message
                .maybe_needs_length
                Binio_printer_helper.With_length64.t]
          ]
          (Bigstring.sub_shared ~pos:metadata_pos ~len:metadata_len buf)
          ~pos:0;
        assert (header_len + metadata_len = len)))
  ;;

  let print_headers ~s_to_c ~c_to_s =
    print_endline "---   client -> server:   ---";
    print_header c_to_s;
    print_endline "---   server -> client:   ---";
    print_header s_to_c
  ;;

  let message_shape bin_shape_payload =
    [%bin_shape:
      payload Binio_printer_helper.With_length.t
        Async_rpc_kernel.Async_rpc_kernel_private.Protocol.Message.maybe_needs_length
        Binio_printer_helper.With_length64.t]
  ;;

  let print_messages t payload_shapes =
    let message_shapes = Nonempty_list.map payload_shapes ~f:message_shape in
    t (fun buf ~pos ~len ->
      let stop = pos + len in
      let rec loop pos =
        if pos = stop
        then ()
        else (
          let message_len =
            Async_rpc_kernel.Async_rpc_kernel_private.Transport.Header.length
            + Bigstring.get_int64_le_exn buf ~pos
          in
          Binio_printer_helper.parse_and_print
            message_shapes
            (Bigstring.sub_shared ~pos ~len:message_len buf)
            ~pos:0;
          let next = pos + message_len in
          if next <> stop then print_endline "";
          loop next)
      in
      loop pos)
  ;;

  let print_messages_bidirectional payload_shapes ~s_to_c ~c_to_s =
    print_endline "---   client -> server:   ---";
    print_messages c_to_s payload_shapes;
    print_endline "---   server -> client:   ---";
    print_messages s_to_c payload_shapes
  ;;
end

let copy_and_tap ~source ~sink ~record_chunk =
  don't_wait_for
    (let%bind (_ : Nothing.t Reader.read_one_chunk_at_a_time_result) =
       Reader.read_one_chunk_at_a_time source ~handle_chunk:(fun reader_buf ~pos ~len ->
         record_chunk ~src:reader_buf ~src_pos:pos ~len;
         Writer.write_bigstring sink reader_buf ~pos ~len;
         let%map () = Writer.flushed sink in
         `Continue)
     in
     Writer.close sink)
;;

let reader_writer_tap () =
  (* We want a reader and writer, and to track the data flowing through the reader and
     writer. Ideally we would just pass a bigstring from the writer to the reader and not
     have files involved but async doesn’t offer that API. Instead, we set up two unix
     pipes and implement our own tee like thing to make a copy of the data we copy from
     one to the next.
     {v
     writer1 ---UNIX--> reader2 ---OCAML---> writer3 ---UNIX--> reader4
       :                             |                             :
       :                             V                             :
       :                           Buffer                          :
       :                             :                             :
       V                             V                             V
     <======================== return values ========================>
     v}

     We could use a pipe transport instead, but the TCP transport is more commonly used
     and so it's nice for the tests to exercise that. *)
  let%map `Reader reader_fd2, `Writer writer_fd1 =
    Unix.pipe (Info.of_string "rpc test connection")
  and `Reader reader_fd4, `Writer writer_fd3 =
    Unix.pipe (Info.of_string "rpc test connection")
  in
  let tap, record_chunk = Tap.create () in
  let writer1 = Writer.create writer_fd1 in
  let reader2 = Reader.create reader_fd2 in
  let writer3 = Writer.create writer_fd3 in
  let reader4 = Reader.create reader_fd4 in
  copy_and_tap ~source:reader2 ~sink:writer3 ~record_chunk;
  writer1, reader4, tap
;;

(* Create an intermediate server that (a) listens on a port and forwards bytes back and
   forth to the given server and (b) gives you [taps] for those connections via a queue.
*)
let tap_server (serv : (Socket.Address.Inet.t, int) Tcp.Server.t) =
  let port = Tcp.Server.listening_on serv in
  let upstream =
    Tcp.Where_to_connect.of_host_and_port { Host_and_port.host = "localhost"; port }
  in
  let conns = Queue.create () in
  let%map server =
    Tcp.Server.create
      ~on_handler_error:`Raise
      Tcp.Where_to_listen.of_port_chosen_by_os
      (fun (_addr : Socket.Address.Inet.t) from_client to_client ->
         let tap_server_to_client, record_chunk_s2c = Tap.create () in
         let tap_client_to_server, record_chunk_c2s = Tap.create () in
         let%bind (_ : _ Socket.t), from_server, to_server = Tcp.connect upstream in
         copy_and_tap ~source:from_client ~sink:to_server ~record_chunk:record_chunk_c2s;
         copy_and_tap ~source:from_server ~sink:to_client ~record_chunk:record_chunk_s2c;
         Queue.enqueue conns (tap_server_to_client, tap_client_to_server);
         Writer.close_finished to_server)
  in
  conns, server
;;

(* This strikes me as a pretty strange function: we set up an ‘rpc connection’ which talks
   to itself. I find it pretty tricky to even think about what is going on here. *)
let with_circular_connection ?lift_implementation ~header ~f () =
  let%bind writer, reader, tap = reader_writer_tap () in
  let implementations =
    let implementations =
      match lift_implementation with
      | None -> implementations
      | Some lift_implementation -> List.map implementations ~f:lift_implementation
    in
    Rpc.Implementations.create_exn
      ~implementations
      ~on_unknown_rpc:`Raise
      ~on_exception:Log_on_background_exn
  in
  let%bind conn =
    with_handshake_header header ~f:(fun () ->
      Rpc.Connection.create ~implementations ~connection_state:(const ()) reader writer
      >>| Result.ok_exn)
  in
  let%bind result = f conn tap in
  let%bind () = Rpc.Connection.close conn in
  return result
;;

let only_heartbeat_once_at_the_beginning =
  Rpc.Connection.Heartbeat_config.create
    ~timeout:(Time_ns.Span.of_sec 360.)
    ~send_every:(Time_ns.Span.of_sec 120.)
    ()
;;

let with_rpc_server_connection
  ?time_source
  ?provide_rpc_shapes
  ?(server_implementations = implementations)
  ?(client_implementations = [])
  ()
  ~server_header
  ~client_header
  ~f
  =
  let server_ivar = Ivar.create () in
  let%bind server =
    with_handshake_header server_header ~f:(fun () ->
      Rpc.Connection.serve
        ?time_source
        ?provide_rpc_shapes
        ~heartbeat_config:only_heartbeat_once_at_the_beginning
        ~implementations:
          (Rpc.Implementations.create_exn
             ~implementations:server_implementations
             ~on_unknown_rpc:`Raise
             ~on_exception:Log_on_background_exn)
        ~initial_connection_state:(fun _ conn ->
          Ivar.fill_exn server_ivar conn;
          ())
        ~where_to_listen:Tcp.Where_to_listen.of_port_chosen_by_os
        ~identification:server_identification
        ())
  in
  let%bind taps, server_proxy = tap_server server in
  let port = Tcp.Server.listening_on server_proxy in
  let where_to_connect =
    Tcp.Where_to_connect.of_host_and_port { Host_and_port.host = "localhost"; port }
  in
  let%bind client =
    with_handshake_header client_header ~f:(fun () ->
      Rpc.Connection.client
        ~heartbeat_config:only_heartbeat_once_at_the_beginning
        ~identification:client_identification
        ~implementations:
          (T
             { connection_state = (fun _ -> ())
             ; implementations =
                 Rpc.Implementations.create_exn
                   ~implementations:client_implementations
                   ~on_unknown_rpc:`Raise
                   ~on_exception:Log_on_background_exn
             })
        where_to_connect
      >>| Result.ok_exn)
  in
  let%bind server_conn = Ivar.read server_ivar in
  let s_to_c, c_to_s = Queue.dequeue_exn taps in
  let%bind result = f ~client ~server:server_conn ~s_to_c ~c_to_s in
  let%bind () = Rpc.Connection.close client in
  let%bind () = Tcp.Server.close server in
  let%bind () = Tcp.Server.close server_proxy in
  return result
;;

let establish_connection
  ?(heartbeat_timeout_style =
    Async_rpc_kernel.Rpc.Connection.Heartbeat_timeout_style.Time_between_heartbeats_legacy)
  transport
  time_source
  description
  ~heartbeat_timeout
  ~heartbeat_every
  =
  let open Expect_test_helpers_core in
  (* Slightly changes the format of print_s to be easier to read in tests *)
  let module Time_ns = Core.Core_private.Time_ns_alternate_sexp in
  (* Prints Time_ns in UTC *)
  let conn =
    Async_rpc_kernel.Rpc.Connection.create
      ~connection_state:(fun _ -> ())
      ~heartbeat_config:
        (Async_rpc_kernel.Rpc.Connection.Heartbeat_config.create
           ~timeout:heartbeat_timeout
           ~send_every:heartbeat_every
           ())
      ~description
      ~time_source
      ~heartbeat_timeout_style
      transport
  in
  Deferred.upon conn (fun conn ->
    let conn = Result.ok_exn conn in
    let () =
      Deferred.upon
        (Async_rpc_kernel.Rpc.Connection.close_reason conn ~on_close:`started)
        (fun reason ->
           print_s
             [%message
               "connection closed"
                 ~now:(Synchronous_time_source.now time_source : Time_ns.t)
                 (description : Info.t)
                 (reason : Info.t)])
    in
    let () =
      Async_rpc_kernel.Rpc.Connection.add_heartbeat_callback conn (fun () ->
        print_endline
          (Sexp.to_string_mach
             [%message
               "received heartbeat"
                 ~now:(Synchronous_time_source.now time_source : Time_ns.t)
                 (description : Info.t)]))
    in
    ());
  conn >>| Result.ok_exn
;;

let setup_server_and_client_connection
  ~heartbeat_timeout
  ~heartbeat_every
  ~heartbeat_timeout_style
  ?(server_heartbeat_timeout = heartbeat_timeout)
  ?(server_heartbeat_every = heartbeat_every)
  ?(server_heartbeat_timeout_style = heartbeat_timeout_style)
  ()
  =
  let server_time_source = Synchronous_time_source.create ~now:Time_ns.epoch () in
  let client_time_source = Synchronous_time_source.create ~now:Time_ns.epoch () in
  let client_transport, server_transport =
    Async_rpc_kernel.Pipe_transport.(create_pair Kind.bigstring)
  in
  let server_conn =
    establish_connection
      server_transport
      (Synchronous_time_source.read_only server_time_source)
      (Info.of_string "server")
      ~heartbeat_timeout:server_heartbeat_timeout
      ~heartbeat_every:server_heartbeat_every
      ~heartbeat_timeout_style:server_heartbeat_timeout_style
  in
  let client_conn =
    establish_connection
      client_transport
      (Synchronous_time_source.read_only client_time_source)
      (Info.of_string "client")
      ~heartbeat_timeout
      ~heartbeat_every
      ~heartbeat_timeout_style
  in
  let%map server_conn, client_conn = Deferred.both server_conn client_conn in
  `Server (server_time_source, server_conn), `Client (client_time_source, client_conn)
;;

module Payload = struct
  type t = int array [@@deriving bin_io]

  let get_random_size () = Random.int 5

  let create () =
    let size = get_random_size () in
    size |> Array.init ~f:(fun (_ : int) -> Random.int 1000)
  ;;
end
