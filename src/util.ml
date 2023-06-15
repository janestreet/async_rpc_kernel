open Core

let dumper_for_message_length_errors = ref (fun (_buf : Bigstring.t) ~pos:(_ : int) -> "")

let dump_and_message buf ~pos =
  match
    try !dumper_for_message_length_errors buf ~pos with
    | exn -> sprintf "Tried to dump message but got exn %s" (Exn.to_string exn)
  with
  | "" -> ""
  | s -> ". " ^ s
;;

(* utility function for bin-io'ing out of a Bigstring.t *)
let bin_read_from_bigstring
      (bin_reader_t : _ Bin_prot.Type_class.reader)
      ?add_len
      buf
      ~pos_ref
      ~(len : Nat0.t)
      ~location
  =
  try
    let init_pos = !pos_ref in
    let data = bin_reader_t.read buf ~pos_ref in
    let add_len =
      match add_len with
      | None -> 0
      | Some add_len -> add_len data
    in
    if !pos_ref - init_pos + add_len <> (len :> int)
    then
      failwithf
        "message length (%d) did not match expected length (%d)%s"
        (!pos_ref - init_pos)
        (len : Nat0.t :> int)
        (dump_and_message buf ~pos:init_pos)
        ();
    Ok data
  with
  | e -> Rpc_result.bin_io_exn ~location e
;;
