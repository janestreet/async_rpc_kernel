open! Core
open! Async

let rpc =
  Rpc.Rpc.create
    ~name:"test_rpc"
    ~version:1
    ~bin_query:bin_unit
    ~bin_response:String.Stable.V1.bin_t
    ~include_in_error_count:Only_on_exn
;;

let custom_metadata_local =
  Type_equal.Id.create
    ~name:"async rpc test metadata"
    [%sexp_of: string * int * string option]
;;

let implementations =
  let implementation =
    Rpc.Rpc.implement rpc (fun () () ->
      let part1 =
        match Async_rpc_kernel.Rpc_metadata.get () with
        | None -> "no legacy request id"
        | Some rqid -> [%string "legacy request %{rqid}"]
      in
      let part2 =
        match Scheduler.find_local custom_metadata_local with
        | None -> "no custom request id"
        | Some x ->
          [%string "custom metadata %{[%sexp (x:string * int* string option)]#Sexp}"]
      in
      return (part1 ^ "; " ^ part2))
  in
  Rpc.Implementations.create_exn
    ~implementations:[ implementation ]
    ~on_unknown_rpc:`Raise
;;

let with_rpc_server_connection ?(implementations = implementations) f ~on_client_connected
  =
  let%bind server =
    Rpc.Connection.serve
      ~implementations
      ~initial_connection_state:(fun _ conn -> on_client_connected conn)
      ~where_to_listen:Tcp.Where_to_listen.of_port_chosen_by_os
      ()
  in
  let port = Tcp.Server.listening_on server in
  let where_to_connect =
    Tcp.Where_to_connect.of_host_and_port { Host_and_port.host = "localhost"; port }
  in
  let%bind conn = Rpc.Connection.client where_to_connect >>| Result.ok_exn in
  let%bind result = f conn in
  let%bind () = Rpc.Connection.close conn
  and () = Tcp.Server.close server in
  return result
;;

let%expect_test "no metadata hooks" =
  let%bind response =
    with_rpc_server_connection
      (fun conn -> Rpc.Rpc.dispatch rpc conn ())
      ~on_client_connected:ignore
  in
  [%sexp_of: string Or_error.t] response |> print_s;
  return [%expect {| (Ok "no legacy request id; no custom request id") |}]
;;

let%expect_test "dispatch with no metadata" =
  let%bind response =
    with_rpc_server_connection
      (fun conn ->
        (match
           Async_rpc_kernel.Async_rpc_kernel_private.Connection.set_metadata_hooks
             conn
             ~when_sending:(fun _ ~query_id:_ -> None)
             ~on_receive:(fun _ ~query_id:_ _ ctx -> ctx)
         with
         | `Ok -> ()
         | `Already_set -> failwith "Unexpected already set");
        Rpc.Rpc.dispatch rpc conn ())
      ~on_client_connected:ignore
  in
  [%sexp_of: string Or_error.t] response |> print_s;
  return [%expect {| (Ok "no legacy request id; no custom request id") |}]
;;

let%expect_test "dispatch with metadata" =
  let%bind response =
    with_rpc_server_connection
      (fun conn ->
        (match
           Async_rpc_kernel.Async_rpc_kernel_private.Connection.set_metadata_hooks
             conn
             ~when_sending:(fun { name; version } ~query_id:_ ->
               Some (name ^ ":" ^ Int.to_string version))
             ~on_receive:(fun _ ~query_id:_ _ ctx -> ctx)
         with
         | `Ok -> ()
         | `Already_set -> failwith "Unexpected already set");
        Rpc.Rpc.dispatch rpc conn ())
      ~on_client_connected:ignore
  in
  [%sexp_of: string Or_error.t] response |> print_s;
  return [%expect {| (Ok "legacy request test_rpc:1; no custom request id") |}]
;;

let%expect_test "dispatch with metadata and implementation hook" =
  let%bind response =
    with_rpc_server_connection
      (fun conn ->
        let without_md = Rpc.Rpc.dispatch rpc conn () in
        (match
           Async_rpc_kernel.Async_rpc_kernel_private.Connection.set_metadata_hooks
             conn
             ~when_sending:(fun { name; version } ~query_id:_ ->
               Some (name ^ ":" ^ Int.to_string version))
             ~on_receive:(fun _ ~query_id:_ _ ctx -> ctx)
         with
         | `Ok -> ()
         | `Already_set -> failwith "Unexpected already set");
        let%map without_md = without_md
        and with_md = Rpc.Rpc.dispatch rpc conn () in
        [ `without_metadata, without_md; `with_metdata, with_md ])
      ~on_client_connected:(fun conn ->
        match
          Async_rpc_kernel.Async_rpc_kernel_private.Connection.set_metadata_hooks
            conn
            ~when_sending:(fun _ ~query_id:_ -> None)
            ~on_receive:(fun { name; version } ~query_id:_ metadata ctx ->
              Async.Execution_context.with_local
                ctx
                custom_metadata_local
                (Some (name, version, metadata)))
        with
        | `Ok -> ()
        | `Already_set -> failwith "Unexpected already set2")
  in
  [%sexp_of: ([ `without_metadata | `with_metdata ] * string Or_error.t) list] response
  |> print_s;
  return
    [%expect
      {|
      ((without_metadata
        (Ok "no legacy request id; custom metadata (test_rpc 1())"))
       (with_metdata
        (Ok "no legacy request id; custom metadata (test_rpc 1(test_rpc:1))")))
      |}]
;;

let%expect_test "expert unknown rpc handler" =
  let implementations =
    Rpc.Implementations.Expert.create_exn
      ~implementations:[]
      ~on_unknown_rpc:
        (`Expert
          (fun ()
               ~rpc_tag
               ~version
               ~metadata
               responder
               (_ : Bigstring.t)
               ~pos:(_ : int)
               ~len ->
            print_s
              [%message
                "Unknown rpc handler called"
                  ~rpc_tag
                  (version : int)
                  (metadata : string option)
                  ~data_len:(len : int)
                  (Scheduler.find_local custom_metadata_local
                    : (string * int * string option) option)];
            Rpc.Rpc.Expert.Responder.write_error
              responder
              (Error.create_s [%message "example error"]);
            return ()))
  in
  let%bind response =
    with_rpc_server_connection
      ~implementations
      (fun conn ->
        (match
           Async_rpc_kernel.Async_rpc_kernel_private.Connection.set_metadata_hooks
             conn
             ~when_sending:(fun { name; version } ~query_id:_ ->
               Some (name ^ ":" ^ Int.to_string version))
             ~on_receive:(fun _ ~query_id:_ _ ctx -> ctx)
         with
         | `Ok -> ()
         | `Already_set -> failwith "Unexpected already set");
        Rpc.Rpc.dispatch rpc conn ())
      ~on_client_connected:(fun conn ->
        match
          Async_rpc_kernel.Async_rpc_kernel_private.Connection.set_metadata_hooks
            conn
            ~when_sending:(fun _ ~query_id:_ -> None)
            ~on_receive:(fun { name; version } ~query_id:_ metadata ctx ->
              print_s
                [%message
                  "on_receive: got metadata"
                    ~name
                    (version : int)
                    (metadata : string option)];
              Async.Execution_context.with_local
                ctx
                custom_metadata_local
                (Some (name, version, metadata)))
        with
        | `Ok -> ()
        | `Already_set -> failwith "Unexpected already set2")
  in
  print_s ([%sexp_of: string Or_error.t] response);
  return
    [%expect
      {|
      ("on_receive: got metadata" (name test_rpc) (version 1)
       (metadata (test_rpc:1)))
      ("Unknown rpc handler called" (rpc_tag test_rpc) (version 1)
       (metadata (test_rpc:1)) (data_len 1)
       ("Scheduler.find_local custom_metadata_local" ((test_rpc 1 (test_rpc:1)))))
      (Error
       ((rpc_error
         (Uncaught_exn
          ((location "server-side raw rpc computation") (exn "example error"))))
        (connection_description ("Client connected via TCP" (localhost PORT)))
        (rpc_name test_rpc) (rpc_version 1)))
      |}]
;;
