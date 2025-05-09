open! Core
open! Async
open! Async_rpc_kernel
open! Async_rpc_kernel_private
open! Expect_test_helpers_core
open! Expect_test_helpers_async

(* bin/test_heartbeat_timeout_override.exe is a binary that just prints the effective
   heartbeat timeout *)
let run env =
  run ~extend_env:env "bash" [ "-c"; "bin/test_heartbeat_timeout_override.exe" ]
;;

let%expect_test "Effective heartbeat timeout with no environment override specified" =
  let%bind () = run [] in
  [%expect {| 30s |}];
  return ()
;;

let%expect_test "Effective heartbeat timeout with an environment override longer than \
                 the default"
  =
  let%bind () = run [ "ASYNC_RPC_HEARTBEAT_TIMEOUT_OVERRIDE", "45s" ] in
  [%expect {| 45s |}];
  return ()
;;

let%expect_test "Effective heartbeat timeout with an environment override shorter than \
                 the default"
  =
  let%bind () = run [ "ASYNC_RPC_HEARTBEAT_TIMEOUT_OVERRIDE", "15s" ] in
  [%expect {| 30s |}];
  return ()
;;

let%expect_test "Effective heartbeat timeout with an invalid environment override" =
  let%bind () = run [ "ASYNC_RPC_HEARTBEAT_TIMEOUT_OVERRIDE", "invalid" ] in
  [%expect
    {|
    ("Unclean exit" (Exit_non_zero 2))
    --- STDERR ---
    Uncaught exception:

      (exn.ml.Reraised "Failed to parse ASYNC_RPC_HEARTBEAT_TIMEOUT_OVERRIDE"
       ("Time_ns.Span.of_string: invalid string" (string invalid)
        (reason "no digits before unit suffix")))
    |}];
  return ()
;;
