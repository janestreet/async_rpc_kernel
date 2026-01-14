open! Core
open! Async

[@@@alert "-legacy_query_metadata"]

let rpc =
  Rpc.Rpc.create
    ~name:"test_rpc"
    ~version:1
    ~bin_query:bin_unit
    ~bin_response:Sexp.Stable.V1.bin_t
    ~include_in_error_count:Only_on_exn
;;

let custom_metadata_local =
  Type_equal.Id.create
    ~name:"async rpc test metadata"
    [%sexp_of: string * int * Async_rpc_kernel.Rpc_metadata.V2.Payload.t option]
;;

let implementations =
  let implementation =
    Rpc.Rpc.implement rpc (fun () () ->
      let legacy_request = Async_rpc_kernel.Rpc_metadata.get_from_context_for_legacy () in
      let custom_metadata = Scheduler.find_local custom_metadata_local in
      return
        [%message
          (legacy_request : Async_rpc_kernel.Rpc_metadata.V2.Payload.t option)
            (custom_metadata
             : (string * int * Async_rpc_kernel.Rpc_metadata.V2.Payload.t option) option)])
  in
  Rpc.Implementations.create_exn
    ~implementations:[ implementation ]
    ~on_unknown_rpc:`Raise
    ~on_exception:(Raise_to_monitor Monitor.main)
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

module%test [@name "single-key tests"] _ = struct
  let%expect_test "no metadata hooks" =
    let%bind () =
      with_rpc_server_connection
        (fun conn -> Rpc.Rpc.dispatch rpc conn ())
        ~on_client_connected:ignore
      >>| ok_exn
      >>| print_s
    in
    [%expect {| ((legacy_request ()) (custom_metadata ())) |}];
    Deferred.unit
  ;;

  let%expect_test "dispatch with no metadata" =
    let%bind () =
      with_rpc_server_connection
        (fun conn ->
          (match
             Async_rpc_kernel.Async_rpc_kernel_private.Connection.set_metadata_hooks
               conn
               ~key:Async_rpc_kernel.Rpc_metadata.V2.Key.default_for_legacy
               ~when_sending:(fun _ ~query_id:_ -> None)
               ~on_receive:(fun _ ~query_id:_ _ ctx -> ctx)
           with
           | `Ok -> ()
           | `Already_set -> failwith "Unexpected already set");
          Rpc.Rpc.dispatch rpc conn ())
        ~on_client_connected:ignore
      >>| ok_exn
      >>| print_s
    in
    [%expect {| ((legacy_request ()) (custom_metadata ())) |}];
    Deferred.unit
  ;;

  let default_when_sending Rpc.Description.{ name; version } ~query_id:_ =
    Some
      (name ^ ":" ^ Int.to_string version
       |> Async_rpc_kernel.Rpc_metadata.V2.Payload.of_string_maybe_truncate)
  ;;

  let default_on_receive Rpc.Description.{ name; version } ~query_id:_ metadata ctx =
    print_s
      [%message
        "on_receive: got metadata"
          ~name
          (version : int)
          (metadata : Async_rpc_kernel.Rpc_metadata.V2.Payload.t option)];
    Async.Execution_context.with_local
      ctx
      custom_metadata_local
      (Some (name, version, metadata))
  ;;

  let%expect_test "dispatch with metadata" =
    let%bind () =
      with_rpc_server_connection
        (fun conn ->
          (match
             Async_rpc_kernel.Async_rpc_kernel_private.Connection.set_metadata_hooks
               conn
               ~key:Async_rpc_kernel.Rpc_metadata.V2.Key.default_for_legacy
               ~when_sending:default_when_sending
               ~on_receive:(fun _ ~query_id:_ _ ctx -> ctx)
           with
           | `Ok -> ()
           | `Already_set -> failwith "Unexpected already set");
          Rpc.Rpc.dispatch rpc conn ())
        ~on_client_connected:ignore
      >>| ok_exn
      >>| print_s
    in
    [%expect {| ((legacy_request (test_rpc:1)) (custom_metadata ())) |}];
    Deferred.unit
  ;;

  let%expect_test "dispatch with metadata and implementation hook" =
    let%bind response =
      with_rpc_server_connection
        (fun conn ->
          let without_md = Rpc.Rpc.dispatch rpc conn () in
          (match
             Async_rpc_kernel.Async_rpc_kernel_private.Connection.set_metadata_hooks
               conn
               ~key:Async_rpc_kernel.Rpc_metadata.V2.Key.default_for_legacy
               ~when_sending:default_when_sending
               ~on_receive:(fun _ ~query_id:_ _ ctx -> ctx)
           with
           | `Ok -> ()
           | `Already_set -> failwith "Unexpected already set");
          let%map without_md
          and with_md = Rpc.Rpc.dispatch rpc conn () in
          [ `without_metadata, without_md; `with_metadata, with_md ])
        ~on_client_connected:(fun conn ->
          match
            Async_rpc_kernel.Async_rpc_kernel_private.Connection.set_metadata_hooks
              conn
              ~key:Async_rpc_kernel.Rpc_metadata.V2.Key.default_for_legacy
              ~when_sending:(fun _ ~query_id:_ -> None)
              ~on_receive:default_on_receive
          with
          | `Ok -> ()
          | `Already_set -> failwith "Unexpected already set2")
    in
    [%sexp_of: ([ `without_metadata | `with_metadata ] * Sexp.t Or_error.t) list] response
    |> print_s;
    return
      [%expect
        {|
        ("on_receive: got metadata" (name test_rpc) (version 1) (metadata ()))
        ("on_receive: got metadata" (name test_rpc) (version 1)
         (metadata (test_rpc:1)))
        ((without_metadata
          (Ok ((legacy_request ()) (custom_metadata ((test_rpc 1 ()))))))
         (with_metadata
          (Ok ((legacy_request ()) (custom_metadata ((test_rpc 1 (test_rpc:1))))))))
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
                    (metadata : Async_rpc_kernel.Rpc_metadata.V2.t option)
                    ~data_len:(len : int)
                    (Scheduler.find_local custom_metadata_local
                     : (string * int * Async_rpc_kernel.Rpc_metadata.V2.Payload.t option)
                         option)];
              Rpc.Rpc.Expert.Responder.write_error
                responder
                (Error.create_s [%message "example error"]);
              return ()))
        ~on_exception:Log_on_background_exn
    in
    let%bind response =
      with_rpc_server_connection
        ~implementations
        (fun conn ->
          (match
             Async_rpc_kernel.Async_rpc_kernel_private.Connection.set_metadata_hooks
               conn
               ~key:Async_rpc_kernel.Rpc_metadata.V2.Key.default_for_legacy
               ~when_sending:default_when_sending
               ~on_receive:(fun _ ~query_id:_ _ ctx -> ctx)
           with
           | `Ok -> ()
           | `Already_set -> failwith "Unexpected already set");
          Rpc.Rpc.dispatch rpc conn ())
        ~on_client_connected:(fun conn ->
          match
            Async_rpc_kernel.Async_rpc_kernel_private.Connection.set_metadata_hooks
              conn
              ~key:Async_rpc_kernel.Rpc_metadata.V2.Key.default_for_legacy
              ~when_sending:(fun _ ~query_id:_ -> None)
              ~on_receive:default_on_receive
          with
          | `Ok -> ()
          | `Already_set -> failwith "Unexpected already set2")
    in
    print_s ([%sexp_of: Sexp.t Or_error.t] response);
    return
      [%expect
        {|
        ("on_receive: got metadata" (name test_rpc) (version 1)
         (metadata (test_rpc:1)))
        ("Unknown rpc handler called" (rpc_tag test_rpc) (version 1)
         (metadata (((0 test_rpc:1)))) (data_len 1)
         ("Scheduler.find_local custom_metadata_local" ((test_rpc 1 (test_rpc:1)))))
        (Error
         ((rpc_error
           (Uncaught_exn
            ((location "server-side raw rpc computation") (exn "example error"))))
          (connection_description ("Client connected via TCP" (localhost PORT)))
          (rpc_name test_rpc) (rpc_version 1)))
        |}]
  ;;
end

module%test [@name "multi-key tests"] _ = struct
  let metadata_key_1, metadata_key_2 =
    Rpc.Rpc_metadata.V2.Key.of_int 0, Rpc.Rpc_metadata.V2.Key.of_int 1
  ;;

  let context_key_1 =
    Univ_map.Key.create
      ~name:"test_metadata_context_key_1"
      [%sexp_of: Async_rpc_kernel.Rpc_metadata.V2.Payload.t]
  ;;

  let context_key_2 =
    Univ_map.Key.create
      ~name:"test_metadata_context_key_2"
      [%sexp_of: Async_rpc_kernel.Rpc_metadata.V2.Payload.t]
  ;;

  let implementations_with_multiple_metadata ?payload_callback () =
    let implementation =
      Rpc.Rpc.implement rpc (fun () () ->
        let payload_1 =
          Async_kernel_scheduler.find_local context_key_1
          |> Option.map ~f:Async_rpc_kernel.Rpc_metadata.V2.Payload.to_string
        in
        let payload_2 =
          Async_kernel_scheduler.find_local context_key_2
          |> Option.map ~f:Async_rpc_kernel.Rpc_metadata.V2.Payload.to_string
        in
        Option.iter payload_callback ~f:(fun f -> f ~payload_1 ~payload_2);
        return [%message (payload_1 : string option) (payload_2 : string option)])
    in
    Rpc.Implementations.create_exn
      ~implementations:[ implementation ]
      ~on_unknown_rpc:`Raise
      ~on_exception:Log_on_background_exn
  ;;

  let set_metadata_hooks_for_client metadata_with_context conn =
    List.iter metadata_with_context ~f:(fun (key, context_key, value) ->
      match
        Async_rpc_kernel.Async_rpc_kernel_private.Connection.set_metadata_hooks
          conn
          ~key
          ~when_sending:(fun (_ : Rpc.Description.t) ~query_id:(_ : Int63.t) ->
            Some (Async_rpc_kernel.Rpc_metadata.V2.Payload.of_string_maybe_truncate value))
          ~on_receive:(fun _ ~query_id:_ _ ctx -> ctx)
      with
      | `Ok -> ()
      | `Already_set ->
        failwithf "Key %s already set for client" (Type_equal.Id.name context_key) ())
  ;;

  let set_metadata_hooks_for_server metadata_with_context conn =
    List.iter metadata_with_context ~f:(fun (key, context_key, (_ : string)) ->
      match
        Async_rpc_kernel.Async_rpc_kernel_private.Connection.set_metadata_hooks
          conn
          ~key
          ~when_sending:(fun (_ : Rpc.Description.t) ~query_id:(_ : Int63.t) -> None)
          ~on_receive:(fun (_ : Rpc.Description.t) ~query_id:(_ : Int63.t) payload ctx ->
            Execution_context.with_local ctx context_key payload)
      with
      | `Ok -> ()
      | `Already_set ->
        failwithf "Key %s already set for server" (Type_equal.Id.name context_key) ())
  ;;

  let%expect_test "hook with multiple metadata keys" =
    let metadata_with_context =
      [ metadata_key_1, context_key_1, "payload 1 from hooks"
      ; metadata_key_2, context_key_2, "payload 2 from hooks"
      ]
    in
    let%bind () =
      with_rpc_server_connection
        ~implementations:(implementations_with_multiple_metadata ())
        (fun conn ->
          set_metadata_hooks_for_client metadata_with_context conn;
          Rpc.Rpc.dispatch rpc conn ())
        ~on_client_connected:(set_metadata_hooks_for_server metadata_with_context)
      >>| ok_exn
      >>| print_s
    in
    [%expect
      {| ((payload_1 ("payload 1 from hooks")) (payload_2 ("payload 2 from hooks"))) |}];
    Deferred.unit
  ;;

  let%expect_test "dispatch with multiple metadata keys without hooks" =
    let metadata_with_context =
      [ metadata_key_1, context_key_1, "payload 1 from hooks"
      ; metadata_key_2, context_key_2, "payload 2 from hooks"
      ]
    in
    let metadata =
      metadata_with_context
      |> List.mapi ~f:(fun i (key, _, (_ : string)) ->
        ( key
        , [%string {|payload %{(i + 1)#Int} from dispatch|}]
          |> Async_rpc_kernel.Rpc_metadata.V2.Payload.of_string_maybe_truncate ))
      |> Async_rpc_kernel.Rpc_metadata.V2.of_alist
      |> ok_exn
    in
    let%bind () =
      with_rpc_server_connection
        ~implementations:(implementations_with_multiple_metadata ())
        (fun conn ->
          Rpc.Rpc.Expert.dispatch_bin_prot_with_metadata' rpc conn () ~metadata
          >>| Rpc.Rpc.rpc_result_to_or_error rpc conn)
        ~on_client_connected:(set_metadata_hooks_for_server metadata_with_context)
      >>| ok_exn
      >>| print_s
    in
    [%expect
      {|
      ((payload_1 ("payload 1 from dispatch"))
       (payload_2 ("payload 2 from dispatch")))
      |}];
    Deferred.unit
  ;;

  let%expect_test "dispatch with multiple metadata keys with some hooks" =
    let metadata_with_context =
      [ metadata_key_1, context_key_1, "payload 1 from hooks"
      ; metadata_key_2, context_key_2, "payload 2 from hooks"
      ]
    in
    let metadata =
      metadata_with_context
      |> List.mapi ~f:(fun i (key, _, (_ : string)) ->
        ( key
        , [%string {|payload %{(i + 1)#Int} from dispatch|}]
          |> Async_rpc_kernel.Rpc_metadata.V2.Payload.of_string_maybe_truncate ))
      |> Async_rpc_kernel.Rpc_metadata.V2.of_alist
      |> ok_exn
    in
    let%bind () =
      with_rpc_server_connection
        ~implementations:(implementations_with_multiple_metadata ())
        (fun conn ->
          (* Here, we only set the metadata hook for [metadata_key_1]. We expect its
             dispatch metadata to be overriden by the metadata hook, while
             [metadata_key_2] will not be overriden *)
          set_metadata_hooks_for_client (metadata_with_context |> List.drop_last_exn) conn;
          Rpc.Rpc.Expert.dispatch_bin_prot_with_metadata' rpc conn () ~metadata
          >>| Rpc.Rpc.rpc_result_to_or_error rpc conn)
        ~on_client_connected:(set_metadata_hooks_for_server metadata_with_context)
      >>| ok_exn
      >>| print_s
    in
    [%expect
      {|
      ((payload_1 ("payload 1 from hooks"))
       (payload_2 ("payload 2 from dispatch")))
      |}];
    Deferred.unit
  ;;

  let%expect_test "hook with metadata payload too large (>1 KiB)" =
    (* The default [max_metadata_size_per_key] when creating a [Connection.t] is 1 KiB.
       This test creates a query metadata value larger than the limit the [truncate]
       function. We expect only the [metadata_key_1] entry to be truncated to bring the
       metadata payload size below the limit. *)
    let metadata_with_context =
      [ metadata_key_1, context_key_1, String.init 2048 ~f:(fun (_ : int) -> 'A')
      ; metadata_key_2, context_key_2, String.init 1024 ~f:(fun (_ : int) -> 'B')
      ]
    in
    let%bind () =
      with_rpc_server_connection
        ~implementations:
          (implementations_with_multiple_metadata
             ~payload_callback:(fun ~payload_1 ~payload_2 ->
               (* Here, we expect the metadata entry for [metadata_key_1] to be truncated
                  to 1 KiB, while the other metadata fields are preserved. *)
               [%test_result: int]
                 (Option.value_exn payload_1 |> String.length)
                 ~expect:1024;
               [%test_result: int]
                 (Option.value_exn payload_2 |> String.length)
                 ~expect:1024)
             ())
        (fun conn ->
          set_metadata_hooks_for_client metadata_with_context conn;
          Rpc.Rpc.dispatch rpc conn ())
        ~on_client_connected:(set_metadata_hooks_for_server metadata_with_context)
      >>| ok_exn
      >>| print_s
    in
    [%expect
      {|
      ((payload_1
        (AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA))
       (payload_2
        (BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB)))
      |}];
    Deferred.unit
  ;;

  let%expect_test "dispatch with metadata payload too large (>1 KiB)" =
    (* The default [max_metadata_size_per_key] when creating a [Connection.t] is 1 KiB.
       This test creates a query metadata value larger than the limit the [truncate]
       function. We expect only the [metadata_key_1] entry to be truncated to bring the
       metadata payload size below the limit. *)
    let metadata_with_context =
      [ metadata_key_1, context_key_1, String.init 2048 ~f:(fun (_ : int) -> 'A')
      ; metadata_key_2, context_key_2, String.init 1024 ~f:(fun (_ : int) -> 'B')
      ]
    in
    let metadata =
      List.map metadata_with_context ~f:(fun (key, _, payload) ->
        key, payload |> Async_rpc_kernel.Rpc_metadata.V2.Payload.of_string_maybe_truncate)
      |> Async_rpc_kernel.Rpc_metadata.V2.of_alist
      |> ok_exn
    in
    let%bind () =
      with_rpc_server_connection
        ~implementations:
          (implementations_with_multiple_metadata
             ~payload_callback:(fun ~payload_1 ~payload_2 ->
               (* Here, we expect the metadata entry for [metadata_key_1] to be truncated
                  to 1 KiB, while the other metadata fields are preserved. *)
               [%test_result: int]
                 (Option.value_exn payload_1 |> String.length)
                 ~expect:1024;
               [%test_result: int]
                 (Option.value_exn payload_2 |> String.length)
                 ~expect:1024)
             ())
        (fun conn ->
          Rpc.Rpc.Expert.dispatch_bin_prot_with_metadata' rpc conn () ~metadata
          >>| Rpc.Rpc.rpc_result_to_or_error rpc conn)
        ~on_client_connected:(set_metadata_hooks_for_server metadata_with_context)
      >>| ok_exn
      >>| print_s
    in
    [%expect
      {|
      ((payload_1
        (AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA))
       (payload_2
        (BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB)))
      |}];
    Deferred.unit
  ;;
end
