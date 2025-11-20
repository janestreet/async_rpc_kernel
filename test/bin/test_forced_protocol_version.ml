open! Core
open! Async

let print_section name =
  Core.printf "======================== %s ========================\n" name
;;

let print_payload_messages tap =
  Async_rpc_kernel_test.Test_helpers.Tap.print_messages
    tap
    [ [%bin_shape: Async_rpc_kernel_test.Test_helpers.Payload.t] ]
;;

(* This exists only to help test ASYNC_RPC_FORCE_PROTOCOL_VERSION_NUMBER_UPPER_BOUND in
   test_connection.ml in the parent directory *)
let () =
  Command.async
    ~summary:
      [%string
        "This command creates an RPC connection with Test_helpers and prints the \
         protocol version from the handshake header, respecting the \
         ASYNC_RPC_FORCE_PROTOCOL_VERSION_NUMBER_UPPER_BOUND environment variable."]
    (let%map_open.Command () = return () in
     fun () ->
       (* Payloads are randomly generated, so do this to avoid nondeterminism. *)
       Random.init 1337;
       Async_rpc_kernel_test.Test_helpers.with_circular_connection
         ~header:Async_rpc_kernel_test.Test_helpers.Header.latest
         ~f:(fun conn tap ->
           print_section "header";
           Async_rpc_kernel_test.Test_helpers.Tap.print_header tap;
           print_section "payload messages";
           let%bind (_response : int array) =
             Rpc.Rpc.dispatch_exn
               Async_rpc_kernel_test.Test_helpers.sort_rpc
               conn
               (Async_rpc_kernel_test.Test_helpers.Payload.create ())
           in
           print_payload_messages tap;
           return ())
         ())
  |> Command_unix.run
;;
