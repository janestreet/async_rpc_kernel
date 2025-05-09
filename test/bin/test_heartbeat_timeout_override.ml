open! Core
open! Async

(* This exists only to help test ASYNC_RPC_HEARTBEAT_TIMEOUT_OVERRIDE in
   test_heartbeat_timeout_override.ml in the parent directory *)
let () =
  Command_unix.run
    (Command.async
       ~summary:
         [%string
           "This command parses ASYNC_RPC_HEARTBEAT_TIMEOUT_OVERRIDE, creates an RPC \
            connection, and prints out the effective heartbeat timeout."]
       (Command.Param.return (fun () ->
          let pipe_reader, pipe_writer = Pipe.create () in
          let info = Info.of_string "test info" in
          let%bind reader = Reader.of_pipe info pipe_reader in
          let%bind writer, _ = Writer.of_pipe info pipe_writer in
          Rpc.Connection.create reader writer ~connection_state:(fun connection_state ->
            print_endline
              [%string.global
                "%{Rpc.Connection.effective_heartbeat_timeout \
                 connection_state#Time_ns.Span}"])
          >>| Result.ok_exn
          >>| ignore)))
;;
