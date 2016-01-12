## 113.24.00

- When `Transfer.Writer.send*` raises, send an error to the client.

- Added `Pipe_rpc` in `Versioned_rpc.Both_convert`.

- Switched to ppx.

- Expose the lower-level registration hook in `Versioned_rpc`.

- Allow custom handling of missed async\_rpc heartbeats.

- Client-side State\_rpc `dispatch` function does not behave well when the reader side
  of the pipe is closed.

  It should gracefully abort the rpc instead of raising exceptions, or whatever it currently does.

- Add `Rpc.Expert.implement` and `Rpc.Expert.implement'` with a similar
  interface as `One_way.Expert.implement` but for 2-way rpcs.

  Exceptions raised by an expert implementations are handled as follow:
  - if the query has already been answered, stop the server (as for one-way expert)
  - if not, send a `Rpc_error.Uncaught_exn` (as for 2-way rpc)

- Adds `Rpc.Pipe_rpc.dispatch_iter`, plus a bunch of additional types to support
  it. The main reason for this is to reduce space usage: `Pipe_rpc.dispatch`
  followed by `Pipe.iter_without_pushback` consumes ~105 words in the steady state
  (i.e., when no messages are coming in) while `dispatch_iter` consumes ~15. I'm
  sure `dispatch` can be improved a lot, but a pipe by itself is 46 words, so it
  can't possibly become as small as `dispatch_iter`.

  Both cases can be made smaller by making `Connection.response_handler` a GADT
  instead of a closure. I plan to do this later.

  One annoying property of the interface is that the only way to cancel
  a subscription is to use `Pipe_rpc.abort`, which has a terrible interface.
  The logical way to improve the interface is to return a record of
  a `Pipe_rpc.t`, a `Connection.t`, and a `Query_id.t`, which allocates an
  additional few words. I'd kind of like to do this but it seems counter to the
  goal of reducing space usage.

- Adds `Rpc.Pipe_rpc.implement_direct`, which uses a "direct stream writer" to
  write results to the other side, rather than using a `Pipe.Writer.t`. This
  consumes much less memory, ~15 words per open query as opposed to ~225 for
  a regular `Pipe_rpc` implementation.

  A large part of the diff in this feature is the addition of a module
  `Implementation_types`, which just contains copies of type definitions from
  `Implementation` and `Implementations`. This is necessary to handle some cyclic
  type definitions (both of these modules now depend on types defined in the other
  module).

  This is the implementation-side dual of `Pipe_rpc.dispatch_iter`.

- Change `Command_rpc.Command` to use `Versioned_rpc.Callee_converts` instead of
  `Versioned_rpc.Both_convert` so that commands can be constructed without client-side
  conversions.  Clients remain free to use conversions or not, as appropriate.

  Removed `val rpcs` from `Callee_converts` interfaces because nothing appears to use it,
  and `Both_convert` does not provide it.  Now `Both_convert.S` can be supplied to satisfy
  `Callee_converts.S`.

- Annotate errors returned by the async-rpc library with the name of the RPC for
  which the error was returned (if it's an rpc-level error) and a description of
  the remote side of the connection (the ip:host if connected via a network
  socket).

- Bring back `val rpcs` in versioned\_rpc modules.

## 113.00.00

- Fixed race in `Rpc` that caused double connection cleanup.

    Two errors, `Connection_closed` and a Writer error,
    `(Uncaught_exn(monitor.ml.Error_((exn(\"writer error\"....))))))`,
    occurring at the same time will cleanup the connection twice and call
    response_handler of open_queries twice with two different errors.

    (((pid 31291) (thread_id 0))
     ((human_readable 2015-05-25T10:47:18+0100)
      (int63_ns_since_epoch 1432547238929886105))
     "unhandled exception in Async scheduler"
     ("unhandled exception"
      ((monitor.ml.Error_
        ((exn
          ("Ivar.fill of full ivar" (Full _)
           lib/async_kernel/src/ivar0.ml:329:14))
         (backtrace
          ("Raised at file \"error.ml\", line 7, characters 21-29"
           "Called from file \"rpc.ml\", line 101, characters 8-31"
           "Called from file \"connection.ml\", line 251, characters 8-172"
           "Called from file \"core_hashtbl.ml\", line 244, characters 36-48"
           "Called from file \"connection.ml\", line 248, characters 2-278"
           "Called from file \"async_stream.ml\", line 49, characters 53-56"
           "Called from file \"async_stream.ml\", line 21, characters 34-39"
           "Called from file \"job_queue.ml\", line 124, characters 4-7" ""))
         (monitor
          (((name main) (here ()) (id 1) (has_seen_error true)
            (is_detached false) (kill_index 0))))))
       ((pid 31291) (thread_id 0)))))

- Fixed bugs in `Rpc` in which a TCP connection's reader was closed before its
  writer.

- In `Versioned_rpc`, eliminated an unnecessary Async cycle when placing RPC
  messages.

- Added `Rpc.Pipe_rpc.close_reason` and `Rpc.State_rpc.close_reason`, which
  give the reason why a pipe returned by an RPC was closed.

    These functions take the IDs that are returned along with the pipes by
    the dispatch functions, so the interface of `dispatch` did not need to
    change.

- Made `Rpc.Expert.dispatch` expose that the connection was closed, just like
  `One_way.Expert.dispatch`.

- Expose the name of the `Versioned_rpc.Menu` RPC.

## 112.35.00

- Moved `Async_extra.Rpc` to its own library, `Async_kernel_rpc`, and
  abstracted its transport layer.

    `Async_kernel_rpc` depends only on `Async_kernel`.  This allows
    `Async_rpc` to be used in javascript or to try transports tuned for
    different use cases.  `Versioned_rpc` was moved to
    `Async_rpc_kernel` as well.

- Added `Rpc.One_way` module, for RPCs that do not expect any response
  or acknowledgment.
- Sped up `Rpc.Connection`.

    We have been able to send 6_000_000 (contentless) one-way messages
    per second under some idealized circumstances.

- In `Rpc`, added an optional `heartbeat_config` argument to configure
  the heartbeat interval and timeout.
- In `Rpc`, added some information to `Connection_closed` exceptions.
- In `Rpc.Implementations.create`'s `on_unknown_rpc` argument, added
  a `connection_state` argument to the `` `Call`` variant.

    When using `Rpc.Connection.serve`, one can put client addresses in
    the `connection_state`, so this makes it possible to identify who
    sent the unknown RPC instead of just saying what the RPC's name and
    version are.

- Added `Rpc.One_way.Expert`, which has an `implement` function that
  gives direct access to the internal buffer instead of using a
  bin-prot reader.
- Made `Rpc` not raise an exception if a connection is closed due to
  heartbeating.
- Reworked `Async_rpc_kernel`'s `Transport` implementation to not
  require a `transfer` function similar to `Async_unix.Writer.transfer`.

    Changed the RPC connection to flush the pipe when the writer is
    closed, which was the only behavior of `Async_unix.Writer.transfer`
    that had been relied on.

    With this change, if someone closes the underlying writer by hand
    the pipes won't be flushed, which should be expected anyway.

- Fixed an issue where "normal" `Pipe_rpc` errors caused the connection
  to shutdown.

    Such errors include querying an unknown RPC or getting an exception
    raised by the RPC implementation shutdown.  Now such errors behave
    like `Rpc` errors, i.e. they are completely ignored besides being
    put in the return value of `Pipe_rpc.dispatch`.

    Errors that occur later in `Pipe_rpc` still cause the connection to
    close, since these should only occur if there is a bug somewhere.

- In `Rpc`, connection-closing errors no longer raise top-level
  exceptions.

    One can now call `close_reason : t -> Info.t Deferred.t` to get the
    reason the connection was closed.

- Added `One_way` rpcs to `Versioned_rpc`.
- Fixed `Rpc` to not write if the transport is closed.

    The fix doesn't check that the writer is closed, but instead ensure
    that we don't try to write when we shouldn't.  This means that it
    will still fail if the user close the transport by hand, which they
    shouldn't do.

    This change requires transport implementations to fail after they
    have been closed.  This is different from the semantics of
    `Async_unix.Writer`, but the latter is non-intuitive and makes it
    hard to check the correctness of the code.  Moreover it is only
    required for `Async_unix.Writer.with_flushed_at_close` which we
    don't use.

