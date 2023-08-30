open Core

let dumper_for_deserialization_errors =
  ref (fun (_buf : Bigstring.t) ~pos:(_ : int) -> "")
;;

let dump_and_message buf ~pos =
  match
    try !dumper_for_deserialization_errors buf ~pos with
    | exn -> sprintf "Tried to dump message but got exn %s" (Exn.to_string exn)
  with
  | "" -> None
  | s -> Some s
;;

exception
  Dumped_buffer_info of
    { info : string
    ; exn : Exn.t
    }

(* utility function for bin-io'ing out of a Bigstring.t *)
let bin_read_from_bigstring
  (bin_reader_t : _ Bin_prot.Type_class.reader)
  ?add_len
  buf
  ~pos_ref
  ~(len : Nat0.t)
  ~location
  =
  let init_pos = !pos_ref in
  try
    let data = bin_reader_t.read buf ~pos_ref in
    let add_len =
      match add_len with
      | None -> 0
      | Some add_len -> add_len data
    in
    if !pos_ref - init_pos + add_len <> (len :> int)
    then (
      let dump =
        match dump_and_message buf ~pos:init_pos with
        | None -> ""
        | Some s -> ". " ^ s
      in
      failwithf
        "message length (%d) did not match expected length (%d)%s"
        (!pos_ref - init_pos)
        (len : Nat0.t :> int)
        dump
        ());
    Ok data
  with
  | e ->
    let e =
      match dump_and_message buf ~pos:init_pos with
      | None -> e
      | Some info -> Dumped_buffer_info { info; exn = e }
    in
    Rpc_result.bin_io_exn ~location e
;;
