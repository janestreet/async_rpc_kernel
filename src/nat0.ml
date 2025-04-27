open! Core

type t = Bin_prot.Nat0.t
[@@deriving bin_shape ~basetype:"899e2f4a-490a-11e6-b68f-bbd62472516c"]

let%expect_test _ =
  print_endline [%bin_digest: t];
  [%expect {| 595f2a60f11816b29a72ddab17002b56 |}]
;;

let bin_t = Bin_prot.Type_class.bin_nat0
let bin_size_t = Bin_prot.Size.bin_size_nat0
let bin_size_t__local = Bin_prot.Size.bin_size_nat0__local
let bin_writer_t = Bin_prot.Type_class.bin_writer_nat0
let bin_write_t = Bin_prot.Write.bin_write_nat0
let bin_write_t__local = Bin_prot.Write.bin_write_nat0__local
let bin_reader_t = Bin_prot.Type_class.bin_reader_nat0
let bin_read_t = Bin_prot.Read.bin_read_nat0

let __bin_read_t__ _buf ~pos_ref _vnat0 =
  Bin_prot.Common.raise_variant_wrong_type "t" !pos_ref
;;

let of_int_exn = Bin_prot.Nat0.of_int

module Option = struct
  type nat0 = t [@@deriving bin_io ~localize]

  module T = struct
    type nonrec t = nat0

    let to_int_exn (t : t) = (t :> int)
    let of_int_exn = of_int_exn
    let sexp_of_t t = to_int_exn t |> [%sexp_of: int]
    let t_of_sexp s = [%of_sexp: int] s |> of_int_exn
    let bin_shape_uuid = Bin_shape.Uuid.of_string "9d113bde-ad07-11ef-bec1-aa19661992c4"
  end

  module Bin_format = struct
    type t = nat0 option [@@deriving bin_io ~localize]

    let bin_read_t__local =
      Bin_prot.Read.bin_read_option__local Bin_prot.Read.bin_read_nat0__local
    ;;
  end

  include (
    Immediate_kernel.Of_intable.Option.Make (T) : Immediate_option.S with type value := t)

  let[@zero_alloc opt] of_option__local (local_ o) =
    match o with
    | None -> none
    | Some v -> some v
  ;;

  let[@zero_alloc opt] to_option__local t = exclave_
    if is_none t then None else Some (unchecked_value t)
  ;;

  let[@zero_alloc opt] bin_read_t buf ~pos_ref =
    (Bin_format.bin_read_t__local buf ~pos_ref |> of_option__local) [@nontail]
  ;;

  let[@zero_alloc opt] bin_read_t__local buf ~pos_ref = bin_read_t buf ~pos_ref

  let[@zero_alloc assume error] __bin_read_t__ _buf ~pos_ref _vnat0 =
    Bin_prot.Common.raise_variant_wrong_type "t" !pos_ref
  ;;

  let[@zero_alloc opt] bin_size_t t =
    (to_option__local t |> Bin_format.bin_size_t__local) [@nontail]
  ;;

  let[@zero_alloc opt] bin_size_t__local t = bin_size_t t

  let[@zero_alloc opt] bin_write_t buf ~pos t =
    (to_option__local t |> Bin_format.bin_write_t__local buf ~pos) [@nontail]
  ;;

  let[@zero_alloc opt] bin_write_t__local buf ~pos t = bin_write_t buf ~pos t
  let bin_writer_t = { Bin_prot.Type_class.size = bin_size_t; write = bin_write_t }
  let bin_reader_t = { Bin_prot.Type_class.read = bin_read_t; vtag_read = __bin_read_t__ }
  let bin_shape_t = Bin_format.bin_shape_t

  let bin_t =
    { Bin_prot.Type_class.shape = bin_shape_t
    ; writer = bin_writer_t
    ; reader = bin_reader_t
    }
  ;;

  let globalize t = t
end
