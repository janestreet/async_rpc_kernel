(** Internal to [Async_rpc_kernel].  See [Rpc.Implementation]. *)

open Core_kernel.Std
open Async_kernel.Std
open Protocol

module F : sig
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

  val lift : 'a t -> f:('b -> 'a) -> 'b t
end

type 'connection_state t =
  { tag     : Rpc_tag.t
  ; version : int
  ; f       : 'connection_state F.t
  }

val description : _ t -> Description.t

val lift : 'a t -> f:('b -> 'a) -> 'b t
