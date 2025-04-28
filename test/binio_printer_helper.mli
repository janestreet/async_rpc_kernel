open! Core

(** A wrapper type to generate a bin_io deserializer that first reads an 8 byte length
    header *)
module With_length64 : sig
  type 'a t = 'a [@@deriving bin_io]
end

(** A wrapper type to generate a bin_io deserializer that first reads an [int] length
    header *)
module With_length : sig
  type 'a t = 'a [@@deriving bin_io, sexp]
end

(** Assuming that the binary data follows one of the given bin shapes, tries to produce
    some human-readable output representing the raw data. Each of the provided shapes will
    be tried in order until one matches. *)
val parse_and_print : Bin_shape.t Nonempty_list.t -> Bigstring.t -> pos:int -> unit
