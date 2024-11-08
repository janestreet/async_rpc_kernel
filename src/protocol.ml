(** Async_rpc protocol types, for internal use only *)

(* WARNING: do not change any of these types without good reason *)

open Bin_prot.Std
open Sexplib.Std
module Core_for_testing = Core
module Rpc_tag : Core.Identifiable = Core.String
module Query_id = Core.Unique_id.Int63 ()

module Unused_query_id : sig
  type t [@@deriving bin_io, sexp_of]

  val singleton : t
end = struct
  type t = Query_id.t [@@deriving bin_io, sexp_of]

  let%expect_test _ =
    print_endline [%bin_digest: t];
    [%expect {| 2b528f4b22f08e28876ffe0239315ac2 |}]
  ;;

  let singleton = Query_id.create ()
end

module Rpc_error : sig
  open Core

  type t =
    | Bin_io_exn of Sexp.t
    | Connection_closed
    | Write_error of Sexp.t
    | Uncaught_exn of Sexp.t
    | Unimplemented_rpc of Rpc_tag.t * [ `Version of int ]
    | Unknown_query_id of Query_id.t
    | Authorization_failure of Sexp.t
    | Message_too_big of Transport.Send_result.message_too_big
    | Unknown of Sexp.t
    | Lift_error of Sexp.t
  [@@deriving bin_io, compare, sexp, variants]

  include Comparable.S with type t := t
end = struct
  module T = struct
    type t =
      | Bin_io_exn of Core.Sexp.t
      | Connection_closed
      | Write_error of Core.Sexp.t
      | Uncaught_exn of Core.Sexp.t
      | Unimplemented_rpc of Rpc_tag.t * [ `Version of Core.Int.Stable.V1.t ]
      | Unknown_query_id of Query_id.t
      | Authorization_failure of Core.Sexp.t
      | Message_too_big of Transport.Send_result.message_too_big
      | Unknown of Core.Sexp.t
      | Lift_error of Core.Sexp.t
    [@@deriving bin_io, sexp, compare, variants]

    let%expect_test "Stable unless we purposefully add a new variant" =
      print_endline [%bin_digest: t];
      [%expect {| 7dc04a86186858eb0bf5008e9a063c21 |}]
    ;;
  end

  let%expect_test "Existing serialization of [Rpc_error] is stable" =
    (* We add new messages unstably to this protocol so it's important that serialization
       is preserved here. I've left indices on all the variants to make this more obvious.
    *)
    let print_bin_prot t =
      Core_for_testing.Bin_prot.Writer.to_bigstring T.bin_writer_t t
      |> Core.Bigstring.Hexdump.to_string_hum
      |> print_endline
    in
    let print f (variant : _ Variantslib.Variant.t) =
      print_endline [%string "%{variant.name}: %{variant.rank#Int}"];
      f variant.constructor |> print_bin_prot
    in
    let sexp = Core.Sexp.of_string "atom" in
    T.Variants.iter
      ~bin_io_exn:(print (fun c -> c sexp))
      ~connection_closed:(print (fun c -> c))
      ~write_error:(print (fun c -> c sexp))
      ~uncaught_exn:(print (fun c -> c sexp))
      ~unimplemented_rpc:(print (fun c -> c (Rpc_tag.of_string "tag") (`Version 0)))
      ~unknown_query_id:(print (fun c -> c (Query_id.of_int_exn 0)))
      ~authorization_failure:(print (fun c -> c sexp))
      ~message_too_big:
        (print (fun c ->
           c ({ size = 2; max_message_size = 1 } : Transport.Send_result.message_too_big)))
      ~unknown:(print (fun c -> c sexp))
      ~lift_error:(print (fun c -> c sexp));
    [%expect
      {|
      Bin_io_exn: 0
      00000000  00 00 04 61 74 6f 6d                              |...atom|
      Connection_closed: 1
      00000000  01                                                |.|
      Write_error: 2
      00000000  02 00 04 61 74 6f 6d                              |...atom|
      Uncaught_exn: 3
      00000000  03 00 04 61 74 6f 6d                              |...atom|
      Unimplemented_rpc: 4
      00000000  04 03 74 61 67 f1 1d 86  94 00                    |..tag.....|
      Unknown_query_id: 5
      00000000  05 00                                             |..|
      Authorization_failure: 6
      00000000  06 00 04 61 74 6f 6d                              |...atom|
      Message_too_big: 7
      00000000  07 02 01                                          |...|
      Unknown: 8
      00000000  08 00 04 61 74 6f 6d                              |...atom|
      Lift_error: 9
      00000000  09 00 04 61 74 6f 6d                              |...atom|
      |}]
  ;;

  include T
  include Core.Comparable.Make (T)
end

module Rpc_result = struct
  type 'a t = ('a, Rpc_error.t) Core.Result.t [@@deriving bin_io, sexp_of]

  let%expect_test _ =
    print_endline [%bin_digest: unit t];
    [%expect {| 3c5c4bdb6a7668a7a89b965be2fac1ba |}]
  ;;
end

module Connection_metadata = struct
  module V1 = struct
    type t =
      { identification : Core.Bigstring.Stable.V1.t option
      ; menu : Menu.Stable.V2.response option
      }
    [@@deriving bin_io, sexp_of]
  end

  module V2 = struct
    type t =
      { identification : Core.Bigstring.Stable.V1.t option
      ; menu : Menu.Stable.V3.response option
      }
    [@@deriving bin_io, sexp_of]

    let of_v1 { V1.identification; menu } =
      { identification; menu = Base.Option.map menu ~f:Menu.of_v2_response }
    ;;

    let to_v1 t : V1.t =
      { identification = t.identification
      ; menu = Base.Option.bind t.menu ~f:Menu.Stable.V3.to_v2_response
      }
    ;;
  end
end

module Header = Protocol_version_header

module Query_v1 = struct
  type 'a needs_length =
    { tag : Rpc_tag.t
    ; version : int
    ; id : Query_id.t
    ; data : 'a
    }
  [@@deriving bin_io, sexp_of]

  let%expect_test _ =
    print_endline [%bin_digest: unit needs_length];
    [%expect {| be5888691d73427b3ac8ea300c169422 |}]
  ;;

  type 'a t = 'a needs_length [@@deriving bin_read]
end

module Query = struct
  type 'a needs_length =
    { tag : Rpc_tag.t
    ; version : int
    ; id : Query_id.t
    ; metadata : string option
    ; data : 'a
    }
  [@@deriving bin_io, sexp_of]

  let%expect_test _ =
    print_endline [%bin_digest: unit needs_length];
    [%expect {| ef70ea2dd0bb812a601d28810e6637d4 |}]
  ;;

  type 'a t = 'a needs_length [@@deriving bin_read]

  let to_v1 { tag; version; id; metadata = (_ : string option); data } : _ Query_v1.t =
    { tag; version; id; data }
  ;;

  let of_v1 ?metadata { Query_v1.tag; version; id; data } =
    { tag; version; id; metadata; data }
  ;;
end

module Response = struct
  type 'a needs_length =
    { id : Query_id.t
    ; data : 'a Rpc_result.t
    }
  [@@deriving bin_io, sexp_of]

  let%expect_test _ =
    print_endline [%bin_digest: unit needs_length];
    [%expect {| e08147cd47ea743ad47cbb4abcd9448d |}]
  ;;

  type 'a t = 'a needs_length [@@deriving bin_read]
end

module Stream_query = struct
  type 'a needs_length =
    [ `Query of 'a
    | `Abort
    ]
  [@@deriving bin_io, sexp_of]

  let%expect_test _ =
    print_endline [%bin_digest: unit needs_length];
    [%expect {| 2c37868761971c78cc355d43f0854860 |}]
  ;;

  type nat0_t = Nat0.t needs_length [@@deriving bin_read, bin_write]
end

module Stream_initial_message = struct
  type ('response, 'error) t =
    { unused_query_id : Unused_query_id.t
    ; initial : ('response, 'error) Core.Result.t
    }
  [@@deriving bin_io, sexp_of]

  let%expect_test _ =
    print_endline [%bin_digest: (unit, unit) t];
    [%expect {| 46f231ddb7fa59da9c27759d50ae01a9 |}]
  ;;
end

module Stream_response_data = struct
  type 'a needs_length =
    [ `Ok of 'a
    | `Eof
    ]
  [@@deriving bin_io, sexp_of]

  let%expect_test _ =
    print_endline [%bin_digest: unit needs_length];
    [%expect {| c1dbcdcfe2b12e797ec64f0d74df1811 |}]
  ;;

  type 'a t = 'a needs_length [@@deriving bin_read, sexp_of]
  type nat0_t = Nat0.t needs_length [@@deriving bin_read, bin_write]
end

module Message = struct
  (* [Close_reason_duplicated] exists because we accidentally rolled [Metadata_v2] without
     preserving backwards compatibility (it was placed in the variants list before
     [Close_reason]), so clients could send [Close_reason] in either index. Luckily,
     [Metadata_v2] wasn't enabled so old clients can't send that message with the
     incorrect index. *)
  type 'a maybe_needs_length =
    | Heartbeat
    | Query_v1 of 'a Query_v1.needs_length
    | Response of 'a Response.needs_length
    | Query of 'a Query.needs_length
    | Metadata of Connection_metadata.V1.t
    | Close_reason of Core.Info.t
    | Close_reason_duplicated of Core.Info.t
    | Metadata_v2 of Connection_metadata.V2.t
  [@@deriving bin_io, sexp_of, variants]

  let%expect_test "Stable unless we purposefully add a new variant" =
    print_endline [%bin_digest: unit maybe_needs_length];
    [%expect {| 34c79522021d12841db0ad5fbf2ecca7 |}]
  ;;

  type 'a t = 'a maybe_needs_length [@@deriving bin_read, sexp_of]
  type nat0_t = Nat0.t maybe_needs_length [@@deriving bin_read, bin_write]

  let%expect_test "Existing serialization of [Message] is stable" =
    (* We add new messages unstably to this protocol so it's important that serialization
       is preserved here. I've left indices on all the variants to make this more obvious.
    *)
    let print_bin_prot t =
      Core_for_testing.Bin_prot.Writer.to_bigstring
        (bin_writer_maybe_needs_length bin_writer_unit)
        t
      |> Core.Bigstring.Hexdump.to_string_hum
      |> print_endline
    in
    let print f (variant : _ Variantslib.Variant.t) =
      print_endline [%string "%{variant.name}: %{variant.rank#Int}"];
      f variant.constructor |> print_bin_prot
    in
    let query : unit Query.needs_length =
      { tag = Rpc_tag.of_string "tag"
      ; version = 0
      ; id = Query_id.of_int_exn 1
      ; metadata = None
      ; data = ()
      }
    in
    let metadata : Connection_metadata.V2.t = { identification = None; menu = None } in
    let close_reason = Core.Info.create_s [%message "Close reason"] in
    Variants_of_maybe_needs_length.iter
      ~heartbeat:(print (fun c -> c))
      ~query_v1:(print (fun c -> c (Query.to_v1 query)))
      ~response:
        (print (fun c ->
           c ({ id = Query_id.of_int_exn 0; data = Ok () } : _ Response.needs_length)))
      ~query:(print (fun c -> c query))
      ~metadata:(print (fun c -> c (Connection_metadata.V2.to_v1 metadata)))
      ~close_reason:(print (fun c -> c close_reason))
      ~close_reason_duplicated:(print (fun c -> c close_reason))
      ~metadata_v2:(print (fun c -> c metadata));
    [%expect
      {|
      Heartbeat: 0
      00000000  00                                                |.|
      Query_v1: 1
      00000000  01 03 74 61 67 00 01 00                           |..tag...|
      Response: 2
      00000000  02 00 00 00                                       |....|
      Query: 3
      00000000  03 03 74 61 67 00 01 00  00                       |..tag....|
      Metadata: 4
      00000000  04 00 00                                          |...|
      Close_reason: 5
      00000000  05 03 00 0c 43 6c 6f 73  65 20 72 65 61 73 6f 6e  |....Close reason|
      Close_reason_duplicated: 6
      00000000  06 03 00 0c 43 6c 6f 73  65 20 72 65 61 73 6f 6e  |....Close reason|
      Metadata_v2: 7
      00000000  07 00 00                                          |...|
      |}]
  ;;
end
