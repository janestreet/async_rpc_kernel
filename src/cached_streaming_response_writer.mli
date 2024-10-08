open! Core
open! Async_kernel

type 'a t

val create : id:Protocol.Query_id.t -> bin_writer:'a Bin_prot.Type_class.writer -> 'a t
val bin_writer : 'a t -> 'a Bin_prot.Type_class.writer
val prep_write : 'a t -> 'a -> ('a t * 'a) Bin_prot.Type_class.writer
val prep_write_expert : 'a t -> len:int -> 'a t Bin_prot.Type_class.writer
val prep_write_string : 'a t -> string -> ('a t * string) Bin_prot.Type_class.writer
