open! Core
open! Async

let synchronous_raising_rpc =
  Rpc.Rpc.create
    ~name:"synchronous-raising"
    ~version:1
    ~bin_query:[%bin_type_class: unit]
    ~bin_response:[%bin_type_class: unit]
    ~include_in_error_count:Only_on_exn
;;

let asynchronous_raising_pipe_rpc =
  Rpc.Pipe_rpc.create
    ~name:"asynchronous-raising"
    ~version:1
    ~bin_query:[%bin_type_class: unit]
    ~bin_response:[%bin_type_class: unit]
    ~bin_error:[%bin_type_class: Error.t]
    ()
;;

let implementations =
  let implementations =
    [ Rpc.Rpc.implement
        synchronous_raising_rpc
        (fun () () ->
          print_endline "Synchronously raising in the impl";
          failwith "Synchronous raise")
        ~on_exception:(Raise_to_monitor Monitor.main)
    ; Rpc.Pipe_rpc.implement
        asynchronous_raising_pipe_rpc
        (fun () () ->
          Pipe.create_reader ~close_on_exception:true (fun writer ->
            let%bind () = Pipe.write writer () in
            let%bind () = Pipe.write writer () in
            print_endline "Asynchronously raising in the impl";
            failwith "Asynchronous raise")
          |> Deferred.Or_error.return)
        ~on_exception:(Raise_to_monitor Monitor.main)
    ]
  in
  Rpc.Implementations.create_exn
    ~implementations
    ~on_unknown_rpc:`Raise
    ~on_exception:Log_on_background_exn
;;

let test_raising_impl_with_detached_monitor_main ~f =
  let next_error = Monitor.detach_and_get_next_error Monitor.main in
  let%bind server =
    Rpc.Connection.serve
      ~implementations
      ~initial_connection_state:(fun (_ : [< Socket.Address.t ]) (_ : Rpc.Connection.t) ->
        ())
      ~where_to_listen:Tcp.Where_to_listen.of_port_chosen_by_os
      ()
  in
  let%bind client_result =
    Rpc.Connection.with_client
      (Tcp.Where_to_connect.of_inet_address (Tcp.Server.listening_on_address server))
      f
    |> Deferred.Or_error.of_exn_result
    >>| Or_error.join
    >>| [%sexp_of: unit Or_error.t]
    >>| Expect_test_helpers_core.remove_backtraces
  in
  let%bind () = Tcp.Server.close server in
  let%map server_exn =
    next_error >>| Exn.sexp_of_t >>| Expect_test_helpers_core.remove_backtraces
  in
  print_s [%message "" (client_result : Sexp.t) (server_exn : Sexp.t)]
;;

let%expect_test "Asynchronously raising with Rpc.On_exception.Raise sends exception to \
                 Monitor.main"
  =
  let%map () =
    test_raising_impl_with_detached_monitor_main ~f:(fun conn ->
      let%bind.Deferred.Or_error pipe, (_ : Rpc.Pipe_rpc.Metadata.t) =
        Rpc.Pipe_rpc.dispatch asynchronous_raising_pipe_rpc conn ()
        |> Deferred.map ~f:Or_error.join
      in
      Pipe.iter_without_pushback pipe ~f:(fun () -> print_endline "Got pipe message")
      |> Deferred.ok)
  in
  [%expect
    {|
    Asynchronously raising in the impl
    Got pipe message
    Got pipe message
    ((client_result (Ok ()))
     (server_exn
      (monitor.ml.Error (Failure "Asynchronous raise") ("ELIDED BACKTRACE"))))
    |}]
;;

let%expect_test "Synchronously raising with Rpc.On_exception.Raise sends exception to \
                 Monitor.main"
  =
  let%map () =
    test_raising_impl_with_detached_monitor_main ~f:(fun conn ->
      Rpc.Rpc.dispatch synchronous_raising_rpc conn ())
  in
  [%expect
    {|
    Synchronously raising in the impl
    ((client_result
      (Error
       ((rpc_error
         (Uncaught_exn
          ((location "server-side rpc computation")
           (exn
            (monitor.ml.Error (Failure "Synchronous raise") ("ELIDED BACKTRACE"))))))
        (connection_description ("Client connected via TCP" 0.0.0.0:PORT))
        (rpc_name synchronous-raising) (rpc_version 1))))
     (server_exn
      (monitor.ml.Error
       ((location "server-side rpc computation")
        (exn
         (monitor.ml.Error (Failure "Synchronous raise") ("ELIDED BACKTRACE"))))
       ("ELIDED BACKTRACE"))))
    |}]
;;
