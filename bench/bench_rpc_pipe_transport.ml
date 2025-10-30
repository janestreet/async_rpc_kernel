include Bench_rpc.Make_benches (struct
    let transport_name = "pipe"

    let create_connection_pair =
      Bench_rpc_implementations.create_connection_pair_with_pipe_transport
    ;;
  end)
