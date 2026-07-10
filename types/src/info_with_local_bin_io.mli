@@ portable

type t = Core.Info.Portable.t [@@deriving bin_io ~localize, globalize, sexp_of]

include module type of Core.Info.Portable with type t := t
include Bin_prot.Binable.S with type t := t

val bin_size_t__local : t @ local -> int
val bin_write_t__local : Bin_prot__Common.buf @ local -> pos:int -> t @ local -> int
