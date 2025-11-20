open! Core
module Uuid = Bin_shape.Uuid

module Int64_long = struct
  type t = int

  let bin_shape_t = Bin_shape.basetype (Uuid.of_string "int64_long") []

  let bin_read_t buf ~pos_ref =
    let i = Bigstring.get_int64_le_exn buf ~pos:!pos_ref in
    pos_ref := !pos_ref + 8;
    i
  ;;

  let bin_write_t buf ~pos t =
    Bigstring.set_int64_le buf ~pos t;
    8
  ;;

  let bin_size_t (_ : int) = 8
end

module Make_with_length (Length : sig
    type t

    val bin_shape_t : Bin_shape.t
    val bin_read_t : Bigstring.t @ local -> pos_ref:int ref @ local -> t
    val bin_write_t : Bigstring.t @ local -> pos:int -> t -> int
    val bin_size_t : t -> int
  end) =
struct
  type 'a t = 'a [@@deriving sexp]

  module Bin = struct
    type 'a t =
      { length : Length.t
      ; body : 'a
      }
    [@@deriving bin_io]
  end

  include
    Binable.Of_binable1_without_uuid
      (Bin)
      (struct
        type nonrec 'a t = 'a t

        let of_binable { Bin.length = _; body } = body
        let to_binable _ = failwith "unused, only for parsing"
      end) [@@alert "-legacy"]
end

module With_length64 = Make_with_length (Int64_long)
module With_length = Make_with_length (Int)

module Row = struct
  (* The rough idea is to build up something roughly grid-shaped where [text] goes in the
     [level+1]th column, bytes go in the -1th column, and [prefix] specifies some extra
     things (e.g. which item you are starting from a list) as their column and contents.

     It isn’t quite tabular because the columns won’t be the same width on every row.
     Rather if you have some structure like:
     {v
     A B C
     D E F G
     H I J K
     L M N
     O P Q R
     v}
     then cells C, G, K, N, R will not affect widths of other cells, and cells F, and J
     will have the same widths as each other but possibly not the same width as Q. Cells
     A, D, H, L, O will have equal widths as will cells B, E, I, M, P. The difference
     between [{F, J}] and [{Q}] is that they are separated by a row which ends on that
     column.

     The second row above would have [{text=G; level=2; prefix = [(1, B); (0, A)]}].
  *)

  type t =
    { bytes : string
    ; level : int
    ; prefix : (int * string) list
    ; text : string
    }
  [@@deriving sexp_of]
end

let row rows buf ~start ~pos ~level ~prefix text =
  Vec.push_back
    rows
    { Row.bytes = Bigstring.To_string.sub buf ~pos:start ~len:(!pos - start)
    ; level
    ; prefix
    ; text
    }
;;

(* Module representing the environment context of a Bin_shape for recursive types

   This is copied directly from app/wildrpc/converters/src/env.ml *)
module Env = struct
  type 'a t = 'a list

  let empty = []
  let set t a = a :: t

  let find_exn t x =
    (* We insert things backwards into the list *)
    let x = List.length t - 1 - x in
    List.nth_exn t x
  ;;
end

(* Custom printers for various types. Maps the ‘uuid’ type identifier (despite the name,
   this is actually often just a string like "int") to a function to turn it into rows.

   Currently these are all registered in [add_base_handlers] below
*)
let base_handlers = String.Table.create ()

(* Read [buf] according to [shape], appending to [rows] *)
let rec parse
  ~(env : Bin_shape.Expert.Canonical.t Env.t)
  (shape : Bin_shape.Expert.Canonical.t)
  rows
  buf
  ~pos
  ~level
  ~prefix
  =
  let (Exp shape) = shape in
  match shape with
  | Annotate (_, shape) -> parse ~env shape rows buf ~pos ~level ~prefix
  | Base (id, args) ->
    let uuid = Uuid.to_string id in
    (match Hashtbl.find base_handlers uuid with
     | Some handler -> handler ~env args rows buf ~pos ~level ~prefix
     | None -> raise_s [%message "unknown uuid" uuid])
  | Tuple contents | Unboxed_tuple contents ->
    parse_tuple ~env contents rows buf ~pos ~level ~prefix
  | Record fields ->
    List.iteri fields ~f:(fun i (name, shape) ->
      let prefix' = level, sprintf "%s=" name in
      let prefix = if i = 0 then prefix' :: prefix else [ prefix' ] in
      parse ~env shape rows buf ~pos ~level:(level + 1) ~prefix)
  | Variant variants ->
    let start = !pos in
    let n = Int.bin_read_t buf ~pos_ref:pos in
    let name, shapes = List.nth_exn variants n in
    row rows buf ~start ~pos ~level ~prefix name;
    parse_tuple ~env shapes rows buf ~pos ~level ~prefix:[]
  | Application (f, args) ->
    let env = Env.set env f in
    List.iteri (f :: args) ~f:(fun i shape ->
      let prefix' = level, " " in
      let prefix = if i = 0 then prefix' :: prefix else [ prefix' ] in
      parse ~env shape rows buf ~pos ~level:(level + 1) ~prefix)
  | Poly_variant table ->
    let start = !pos in
    let code = Bin_prot.Read.bin_read_variant_int buf ~pos_ref:pos in
    let constructors = Bin_shape.Expert.Sorted_table.expose table in
    (match
       List.find constructors ~f:(fun (name, _) ->
         Ocaml_common.Btype.hash_variant name = code)
     with
     | None ->
       raise_s
         [%message
           "Unrecognized variant code"
             (code : int)
             ~constructors:(List.map constructors ~f:fst : string list)]
     | Some (name, shape) ->
       row rows buf ~start ~pos ~level ~prefix ("`" ^ name);
       (match shape with
        | None -> ()
        | Some shape -> parse ~env shape rows buf ~pos ~level ~prefix))
  | Rec_app (x, []) ->
    let shape = Env.find_exn env x in
    parse ~env shape rows buf ~pos ~level ~prefix
  | Rec_app (x, args) ->
    raise_s
      [%message
        "Recursive types with type arguments not implemented"
          (x : int)
          (args : Bin_shape.Expert.Canonical.t list)]
  | Var x -> raise_s [%message "Var types are not implemented" (x : int)]

and parse_tuple ~env contents rows buf ~pos ~level ~prefix =
  match contents with
  | [ shape ] ->
    (* We often end up here for a variant with one arg *)
    parse ~env shape rows buf ~pos ~level ~prefix
  | contents ->
    let first_prefix = (level, "1.") :: prefix in
    List.iteri contents ~f:(fun i shape ->
      let i = i + 1 in
      parse
        ~env
        shape
        rows
        buf
        ~pos
        ~level:(level + 1)
        ~prefix:(if i = 1 then first_prefix else [ level, sprintf "%d." i ]))
;;

let handle0 name f =
  Hashtbl.add_exn base_handlers ~key:name ~data:(fun ~env:_ -> function
    | [] -> f
    | args ->
      failwithf
        !"Wrong number of args for %s. Expected 0. Got %{sexp: Bin_shape.Canonical.t \
          list}"
        name
        args
        ())
;;

let handle1 name f =
  Hashtbl.add_exn base_handlers ~key:name ~data:(fun ~env -> function
    | [ arg ] -> f ~env arg
    | args ->
      failwithf
        !"Wrong number of args for %s. Expected 1. Got %{sexp: Bin_shape.Canonical.t \
          list}"
        name
        args
        ())
;;

let handlecopy ~from name =
  Hashtbl.add_exn base_handlers ~key:name ~data:(Hashtbl.find_exn base_handlers from)
;;

let add_base_handlers () =
  (* Note that the ‘uuid’ for int and suchlike is just "int". *)
  handle0 "unit" (fun rows buf ~pos ~level ~prefix ->
    let start = !pos in
    Unit.bin_read_t buf ~pos_ref:pos;
    row rows buf ~start ~pos ~level ~prefix "()");
  handle0 "int" (fun rows buf ~pos ~level ~prefix ->
    let start = !pos in
    let n = Int.bin_read_t buf ~pos_ref:pos in
    row rows buf ~start ~pos ~level ~prefix (sprintf "%d (int)" n));
  handle0 "899e2f4a-490a-11e6-b68f-bbd62472516c" (fun rows buf ~pos ~level ~prefix ->
    let start = !pos in
    let n = Bin_prot.Read.bin_read_nat0 buf ~pos_ref:pos in
    row rows buf ~start ~pos ~level ~prefix (sprintf "%d (nat0)" (n :> int)));
  handlecopy "int63" ~from:"int";
  handle0 "int64_long" (fun rows buf ~pos ~level ~prefix ->
    let start = !pos in
    let n = Int64_long.bin_read_t buf ~pos_ref:pos in
    row rows buf ~start ~pos ~level ~prefix (sprintf "%d (64-bit LE)" n));
  handle0 "string" (fun rows buf ~pos ~level ~prefix ->
    let start = !pos in
    let str = String.bin_read_t buf ~pos_ref:pos in
    row
      rows
      buf
      ~start
      ~pos
      ~level
      ~prefix
      (sprintf
         !"%{sexp: string} (%d bytes)"
         (let len_limit = 10 in
          if String.length str > len_limit
          then String.prefix str (len_limit - 3) ^ "..."
          else str)
         (String.length str)));
  handlecopy "bigstring" ~from:"string";
  (* MD5: *)
  handle0 "f6bdcdd0-9f75-11e6-9a7e-d3020428efed" (fun rows buf ~pos ~level ~prefix ->
    let start = !pos in
    let (_ : Md5.t) = Md5.bin_read_t buf ~pos_ref:pos in
    row rows buf ~start ~pos ~level ~prefix "(md5)");
  let list_or_array ~env name =
    let item_count n = sprintf "%s: %d items" name n in
    fun shape rows buf ~pos ~level ~prefix ->
      let start = !pos in
      let n = Int.bin_read_t buf ~pos_ref:pos in
      row rows buf ~start ~pos ~level ~prefix (item_count n);
      for i = 0 to n - 1 do
        let prefix = [ level, sprintf "%d:" i ] in
        parse ~env shape rows buf ~pos ~level:(level + 1) ~prefix
      done
  in
  handle1 "list" (list_or_array "List");
  handle1 "array" (list_or_array "Array");
  handle1 "option" (fun ~env shape rows buf ~pos ~level ~prefix ->
    let start = !pos in
    let some = Bool.bin_read_t buf ~pos_ref:pos in
    row rows buf ~start ~pos ~level ~prefix (if some then "Some" else "None");
    if some then parse ~env shape rows buf ~pos ~level:(level + 1) ~prefix:[])
;;

let () = add_base_handlers ()

module Aligned_row = struct
  module Simplified_row = struct
    type t =
      { bytes : string
      ; parts : string array
      }

    let of_rows (rows : Row.t Vec.t) =
      let out = Array.create ~len:(Vec.length rows) { bytes = ""; parts = [||] } in
      Vec.iteri rows ~f:(fun i { bytes; level; prefix; text } ->
        let parts = Array.create ~len:(level + 1) "" in
        parts.(level) <- text;
        List.iter prefix ~f:(fun (level, str) -> parts.(level) <- str);
        out.(i) <- { bytes; parts });
      out
    ;;
  end

  type t =
    { bytes : string
    ; body : string
    }

  let align (rows : Row.t Vec.t) =
    let simplified = Simplified_row.of_rows rows in
    let limit = Array.length simplified in
    for i = -1 to limit - 2 do
      let previous_len =
        if i >= 0
        then (
          let parent = simplified.(i) in
          Array.length parent.parts)
        else 1
      in
      for prefix_len = previous_len to Array.length simplified.(i + 1).parts do
        let rec loop j max_prefix =
          if j < limit
          then (
            let child = simplified.(j) in
            if Array.length child.parts > prefix_len
            then
              loop (j + 1) (max max_prefix (String.length child.parts.(prefix_len - 1)))
            else max_prefix, j - 1)
          else max_prefix, j - 1
        in
        let max_prefix, last_child = loop (i + 1) 0 in
        for j = i + 1 to last_child do
          simplified.(j).parts.(prefix_len - 1)
          <- sprintf "%*s " max_prefix simplified.(j).parts.(prefix_len - 1)
        done
      done
    done;
    Array.map simplified ~f:(fun { bytes; parts } ->
      { bytes; body = String.concat_array parts })
  ;;

  let print (ts : t array) =
    Array.iter ts ~f:(fun { bytes; body } ->
      let print_line bytes description = printf "%-22s %s\n" bytes description in
      let print_line' body fmt = ksprintf (fun s -> print_line s body) fmt in
      let c i str = String.get str i |> Char.to_int in
      let rec print_one b body =
        (* Obviously, this could be done in a more elegant way, but this is at least
           simple *)
        match String.length b with
        | 0 -> print_line "" body
        | 1 -> print_line' body "%02x" (c 0 b)
        | 2 -> print_line' body "%02x%02x" (c 0 b) (c 1 b)
        | 3 -> print_line' body "%02x%02x %02x" (c 0 b) (c 1 b) (c 2 b)
        | 4 -> print_line' body "%02x%02x %02x%02x" (c 0 b) (c 1 b) (c 2 b) (c 3 b)
        | 5 ->
          print_line'
            body
            "%02x%02x %02x%02x %02x"
            (c 0 b)
            (c 1 b)
            (c 2 b)
            (c 3 b)
            (c 4 b)
        | 6 ->
          print_line'
            body
            "%02x%02x %02x%02x %02x%02x"
            (c 0 b)
            (c 1 b)
            (c 2 b)
            (c 3 b)
            (c 4 b)
            (c 5 b)
        | 7 ->
          print_line'
            body
            "%02x%02x %02x%02x %02x%02x %02x"
            (c 0 b)
            (c 1 b)
            (c 2 b)
            (c 3 b)
            (c 4 b)
            (c 5 b)
            (c 6 b)
        | len ->
          let body' = if len = 8 then body else "..." in
          print_line'
            body'
            "%02x%02x %02x%02x %02x%02x %02x%02x"
            (c 0 b)
            (c 1 b)
            (c 2 b)
            (c 3 b)
            (c 4 b)
            (c 5 b)
            (c 6 b)
            (c 7 b);
          if len > 8 then print_one (String.drop_prefix b 8) body
      in
      print_one bytes body)
  ;;
end

let try_parse_and_print (shape : Bin_shape.t) buf ~pos =
  let rows = Vec.create () in
  let posref = ref pos in
  (try
     parse ~env:Env.empty (Bin_shape.eval shape) rows buf ~pos:posref ~level:0 ~prefix:[]
   with
   | exn ->
     let backtrace = Backtrace.Exn.most_recent () in
     raise_s
       [%message
         "Failure in binio_printer_helper.ml"
           ~start:(pos : int)
           (posref : int ref)
           (exn : Exn.t)
           (backtrace : Backtrace.t)
           ~before_error:
             (Bigstring.sub buf ~pos ~len:(!posref - pos) : Bigstring.Hexdump.t)
           ~after_error:(Bigstring.subo buf ~pos:!posref : Bigstring.Hexdump.t)]);
  rows |> Aligned_row.align |> Aligned_row.print
;;

let parse_and_print shapes buf ~pos =
  let rec loop shapes exns =
    match shapes with
    | [] -> raise_s [%message "No parsing found" (exns : exn list)]
    | shape :: rest ->
      (try try_parse_and_print shape buf ~pos with
       | exn -> loop rest (exn :: exns))
  in
  loop (Nonempty_list.to_list shapes) []
;;
