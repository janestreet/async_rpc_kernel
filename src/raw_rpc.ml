open! Core
open! Async_kernel

let dispatch'
  connection
  ~tag
  ~version
  ~bin_writer_query
  ~query
  ~query_id
  ~metadata
  ~response_handler
  =
  let query =
    { Protocol.Query.Validated.tag
    ; version
    ; id = query_id
    ; metadata =
        Connection.compute_metadata
          connection
          { name = Protocol.Rpc_tag.to_string tag; version }
          query_id
          ~dispatch_metadata:metadata
    ; data = query
    }
  in
  match
    Connection.dispatch connection ~response_handler ~kind:Query ~bin_writer_query ~query
  with
  | Ok result -> Ok result
  | Error Closed -> Error Rpc_error.Connection_closed
  | Error (Message_too_big message_too_big) ->
    Error (Rpc_error.Message_too_big message_too_big)
;;

let dispatch connection ~tag ~version ~bin_writer_query ~query ~query_id ~metadata ~f =
  let response_ivar = Ivar.create () in
  (match
     dispatch'
       connection
       ~tag
       ~version
       ~bin_writer_query
       ~query
       ~query_id
       ~metadata
       ~response_handler:(Some (f response_ivar))
   with
   | Ok () -> ()
   | Error _ as e -> Ivar.fill_exn response_ivar e);
  Ivar.read response_ivar
;;
