type t = Core.Info.t [@@deriving bin_io ~localize, globalize, sexp_of]

include Core.Info.S with type t := t
include Bin_prot.Binable.S with type t := t

val bin_size_t__local : local_ t -> int
val bin_write_t__local : local_ Bin_prot__Common.buf -> pos:int -> local_ t -> int
