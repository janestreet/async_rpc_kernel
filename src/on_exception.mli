open! Core
open! Async_kernel

module Exception_type : sig
  type t =
    | Raised_before_implementation_returned
    | Raised_after_implementation_returned (* [Monitor.try_with ~rest] *)
  [@@deriving compare ~localize, sexp_of]
end

module Background_monitor_rest : sig
  type t =
    [ `Log
    | `Call of exn -> unit
    ]

  module Expert : sig
    val merge : [ `Call of exn -> unit ] -> t option -> t
  end
end

(** In all cases except for [Raise_to_monitor], if the exception was raised before the
    implementation returned then it gets sent to the client as an [Uncaught_exn]. *)
type t =
  | Call of (Exception_type.t -> exn -> Description.t -> unit)
  (** [Exception_type] represents whether the exception was raised before or after the
      implementation returned. When the callback itself is executing we could have already
      written a response to the peer so it doesn't represent the current state of the
      system (rather the state of the system when the exception was raised).

      Note that the callback in [Call] will be invoked within the monitor of the RPC
      connection. This means that if [Call] raises, it will be caught by the connection's
      monitor and shut down the connection, but will not propagate further. If you'd like
      the connection to propagate elsewhere, you should use [Monitor.send_exn] with a
      different monitor. *)
  | Log_on_background_exn
  (** Logs the exception if it was raised after the implementation returned. *)
  | Close_connection
  | Raise_to_monitor of Monitor.t
  (** Raises the exception to the given monitor. To shut down the whole program you can
      provide [Monitor.main]. Note that this applies both if the exception is thrown
      before the implementation returns or afterwards. *)
[@@deriving sexp_of]

(** Handle an exn that was raised before the implementation returned. It's possible that
    this function is called after we write a response to the peer but it's referring to an
    earlier exception. In that scenario the return type isn't really relevant since we've
    already written a response. *)
val handle_exn_before_implementation_returns
  :  t
  -> Exn.t
  -> Description.t
  -> close_connection_monitor:Monitor.t
  -> [ `Stop | `Continue ]

val to_background_monitor_rest
  :  t
  -> Description.t
  -> close_connection_monitor:Monitor.t
  -> [> Background_monitor_rest.t ] option
