open Core_kernel.Std
open Async_kernel.Std
open Protocol

module F = struct
  type 'connection_state t =
    | One_way
      : 'msg Bin_prot.Type_class.reader
        * ('connection_state -> 'msg -> unit )
      -> 'connection_state t
    | One_way_expert
      : ('connection_state -> Bigstring.t -> pos : int -> len : int -> unit)
      -> 'connection_state t
    | Blocking_rpc
      : 'query Bin_prot.Type_class.reader
        * 'response Bin_prot.Type_class.writer
        * ('connection_state -> 'query -> 'response)
      -> 'connection_state t
    | Rpc
      : 'query Bin_prot.Type_class.reader
        * 'response Bin_prot.Type_class.writer
        * ('connection_state -> 'query -> 'response Deferred.t)
      -> 'connection_state t
    | Pipe_rpc
      : 'query Bin_prot.Type_class.reader
    (* 'init can be an error or an initial state *)
        * 'init Bin_prot.Type_class.writer
        * 'update Bin_prot.Type_class.writer
        * ('connection_state
           -> 'query
           -> aborted:unit Deferred.t
           -> ('init * 'update Pipe.Reader.t, 'init) Result.t Deferred.t
          )
      -> 'connection_state t

  let lift t ~f =
    match t with
    | One_way (bin_msg, impl) ->
      One_way (bin_msg, fun state str -> impl (f state) str)
    | One_way_expert impl ->
      One_way_expert (fun state buf ~pos ~len -> impl (f state) buf ~pos ~len)
    | Blocking_rpc (bin_query, bin_response, impl) ->
      Blocking_rpc (bin_query, bin_response, fun state q -> impl (f state) q)
    | Rpc (bin_query, bin_response, impl) ->
      Rpc (bin_query, bin_response, fun state q -> impl (f state) q)
    | Pipe_rpc (bin_q, bin_i, bin_u, impl) ->
      Pipe_rpc (bin_q, bin_i, bin_u, fun state q ~aborted -> impl (f state) q ~aborted)
end

type 'connection_state t =
  { tag     : Rpc_tag.t
  ; version : int
  ; f       : 'connection_state F.t
  }

let description t = { Description. name = Rpc_tag.to_string t.tag; version = t.version }

let lift t ~f = { t with f = F.lift ~f t.f }
