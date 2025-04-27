open! Core
open! Async_kernel

type 'a t

val create
  :  Protocol_writer.t
  -> Protocol.Query_id.t
  -> Protocol.Impl_menu_index.t
  -> Description.t
  -> bin_writer:'a Bin_prot.Type_class.writer
  -> 'a t

val bin_writer : 'a t -> 'a Bin_prot.Type_class.writer
val flushed : 'a t -> unit Deferred.t
val write : 'a t -> 'a -> unit
val write_string : 'a t -> string -> unit
val write_expert : 'a t -> buf:Bigstring.t -> pos:int -> len:int -> unit

val schedule_write_expert
  :  here:[%call_pos]
  -> 'a t
  -> buf:Bigstring.t
  -> pos:int
  -> len:int
  -> [ `Flushed of unit Deferred.t Modes.Global.t | `Closed ]
