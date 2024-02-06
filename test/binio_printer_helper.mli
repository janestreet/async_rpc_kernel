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

(** Assuming that the binary data follows the given bin shape, tries to produce some
    human-readable output representing the raw data. *)
val parse_and_print : Bin_shape.t -> Bigstring.t -> pos:int -> unit
