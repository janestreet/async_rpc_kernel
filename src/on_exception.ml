open! Core
open! Async_kernel

module Exception_type = struct
  type t =
    | Raised_before_implementation_returned
    | Raised_after_implementation_returned
  (* [Monitor.try_with ~rest] *) [@@deriving compare ~localize, sexp_of]
end

module Background_monitor_rest = struct
  type t =
    [ `Log
    | `Call of exn -> unit
    ]

  module Expert = struct
    let merge (`Call callback) t =
      `Call
        (fun exn ->
          callback exn;
          match t with
          | None -> ()
          | Some (`Call second_callback) -> second_callback exn
          | Some `Log -> !Monitor.Expert.try_with_log_exn exn)
    ;;
  end
end

type t =
  | Call of (Exception_type.t -> exn -> Description.t -> unit)
  | Log_on_background_exn
  | Close_connection
  | Raise_to_monitor of Monitor.t
[@@deriving sexp_of]

let handle_exn_before_implementation_returns t exn description ~close_connection_monitor =
  match t with
  | Call callback ->
    callback Raised_before_implementation_returned exn description;
    `Continue
  | Log_on_background_exn -> `Continue
  | Close_connection ->
    Monitor.send_exn close_connection_monitor exn;
    `Stop
  | Raise_to_monitor monitor ->
    Monitor.send_exn monitor exn;
    `Continue
;;

let to_background_monitor_rest t description ~close_connection_monitor =
  match t with
  | Call callback ->
    Some
      (`Call (fun exn -> callback Raised_after_implementation_returned exn description))
  | Log_on_background_exn -> Some `Log
  | Close_connection -> Some (`Call (Monitor.send_exn close_connection_monitor))
  | Raise_to_monitor monitor -> Some (`Call (Monitor.send_exn monitor))
;;
