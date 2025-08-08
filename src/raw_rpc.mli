open! Core
open! Async_kernel

val dispatch'
  :  Connection.t
  -> tag:Protocol.Rpc_tag.t
  -> version:int
  -> bin_writer_query:'a Bin_prot.Type_class.writer0
  -> query:'a
  -> query_id:Protocol.Query_id.t
  -> metadata:Rpc_metadata.V2.t
  -> response_handler:Connection.Response_handler.t option
  -> (unit, Rpc_error.t) result

val dispatch
  :  Connection.t
  -> tag:Protocol.Rpc_tag.t
  -> version:int
  -> bin_writer_query:'a Bin_prot.Type_class.writer0
  -> query:'a
  -> query_id:Protocol.Query_id.t
  -> metadata:Rpc_metadata.V2.t
  -> f:(('b, Rpc_error.t) result Ivar.t -> Connection.Response_handler.t)
  -> ('b, Rpc_error.t) result Deferred.t
