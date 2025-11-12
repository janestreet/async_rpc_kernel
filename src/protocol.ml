(** Async_rpc protocol types, for internal use only *)

(* WARNING: do not change any of these types without good reason *)

open Bin_prot.Std
open Sexplib.Std
module Core_for_testing = Core

include struct
  open Core

  let%template compare_int = (compare_int [@mode m]) [@@mode m = (global, local)]
  let globalize_int = globalize_int
  let globalize_option = globalize_option
  let globalize_string = globalize_string
end

module Rpc_tag : sig
  include%template Core.Identifiable [@mode local]

  val globalize : local_ t -> t
  val of_string_local : local_ string -> local_ t
  val to_string_local : local_ t -> local_ string
end = struct
  include Core.String

  let of_string_local str = str
  let to_string_local str = str
end

module Query_id = struct
  include Core.Unique_id.Int63 ()

  let globalize (local_ t) : t = t
  let bin_size_t__local (local_ t) = bin_size_t (globalize t)
  let bin_write_t__local (local_ buf) ~pos (local_ t) = bin_write_t buf ~pos (globalize t)
end

module Unused_query_id : sig
  type t [@@deriving bin_io ~localize, sexp_of]

  val singleton : t
end = struct
  type t = Query_id.t [@@deriving bin_io ~localize, sexp_of]

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
  [@@deriving bin_io ~localize, compare ~localize, globalize, sexp, variants]

  include Comparable.S with type t := t
end = struct
  module T = struct
    type t =
      | Bin_io_exn of Core.Sexp.t
      | Connection_closed
      | Write_error of Core.Sexp.t
      | Uncaught_exn of Core.Sexp.t
      | Unimplemented_rpc of Rpc_tag.t * [ `Version of int ]
      | Unknown_query_id of Query_id.t
      | Authorization_failure of Core.Sexp.t
      | Message_too_big of Transport.Send_result.message_too_big
      | Unknown of Core.Sexp.t
      | Lift_error of Core.Sexp.t
    [@@deriving bin_io ~localize, compare ~localize, globalize, sexp, variants]

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
  type 'a t = ('a, Rpc_error.t) Core.Result.t
  [@@deriving bin_io ~localize, globalize, sexp_of]

  let%expect_test _ =
    print_endline [%bin_digest: unit t];
    [%expect {| 3c5c4bdb6a7668a7a89b965be2fac1ba |}]
  ;;
end

module Connection_metadata = struct
  module Stable_bigstring_v1_with_globalize = struct
    include Core.Bigstring.Stable.V1

    let globalize = Core.Bigstring.globalize
  end

  module V1 = struct
    type t =
      { identification : Stable_bigstring_v1_with_globalize.t option
      ; menu : Menu.Stable.V2.response option
      }
    [@@deriving bin_io ~localize, globalize, sexp_of]
  end

  module V2 = struct
    type t =
      { identification : Stable_bigstring_v1_with_globalize.t option
      ; menu : Menu.Stable.V3.response option
      }
    [@@deriving bin_io ~localize, globalize, sexp_of]

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

module Query = struct
  module V1 = struct
    type 'a needs_length =
      { tag : Rpc_tag.t
      ; version : int
      ; id : Query_id.t
      ; data : 'a
      }
    [@@deriving bin_io ~localize, globalize, sexp_of]

    let%expect_test _ =
      print_endline [%bin_digest: unit needs_length];
      [%expect {| be5888691d73427b3ac8ea300c169422 |}]
    ;;

    type 'a t = 'a needs_length [@@deriving bin_read]
  end

  module V2 = struct
    type 'a needs_length =
      { tag : Rpc_tag.t
      ; version : int
      ; id : Query_id.t
      ; metadata : Rpc_metadata.V1.t option
      ; data : 'a
      }
    [@@deriving bin_io ~localize, globalize, sexp_of]

    let%expect_test _ =
      print_endline [%bin_digest: unit needs_length];
      [%expect {| ef70ea2dd0bb812a601d28810e6637d4 |}]
    ;;

    type 'a t = 'a needs_length [@@deriving bin_read]
  end

  module V3 = struct
    type 'a needs_length =
      { tag : Rpc_tag.t
      ; version : int
      ; id : Query_id.t
      ; metadata : Rpc_metadata.V2.t option
      ; data : 'a
      }
    [@@deriving bin_io ~localize, globalize, sexp_of]

    let%expect_test _ =
      print_endline [%bin_digest: unit needs_length];
      [%expect {| 353c0fbd857bef60bcc518055e464468 |}]
    ;;

    type 'a t = 'a needs_length [@@deriving bin_read]
  end

  module V4 = struct
    module Rpc_specifier = struct
      type t =
        | Tag_and_version of Rpc_tag.t * int
        | Rank of int
      [@@deriving bin_io ~localize, globalize, sexp_of]
    end

    type 'a needs_length =
      { specifier : Rpc_specifier.t
      ; id : Query_id.t
      ; metadata : Rpc_metadata.V2.t option
      ; data : 'a
      }
    [@@deriving bin_io ~localize, globalize, sexp_of]

    let%expect_test _ =
      print_endline [%bin_digest: unit needs_length];
      [%expect {| 779fa3aaff407a16d3e4444349604e54 |}]
    ;;

    type 'a t = 'a needs_length [@@deriving bin_read]
  end

  module Validated = struct
    (* NB: that this seems like it should just be type-equal to [V3.t], but subsequent
       versions of the Query wire message allow for specifying the rank of the RPC (the
       index of the RPC + version in the callee menu), so this Validated type encodes that
       we've received such a message and have appropriately translated it into a known RPC
       tag and version. *)
    type 'a t =
      { tag : Rpc_tag.t
      ; version : int
      ; id : Query_id.t
      ; metadata : Rpc_metadata.V2.t option
      ; data : 'a
      }
    [@@deriving globalize, sexp_of]

    [%%template
    [@@@alloc.default a @ m = (heap @ global, stack @ local)]

    let of_v4 (v4_query : 'a V4.t) ~description : 'a t =
      let { V4.specifier = _; id; metadata; data } = v4_query in
      let { Description.name; version } = description in
      { tag = Rpc_tag.of_string name; version; id; metadata; data }
    ;;

    let of_v4__local (local_ (v4_query : 'a V4.t)) ~(local_ tag) ~version : 'a t
      = exclave_
      let { V4.specifier = _; id; metadata; data } = v4_query in
      { tag; version; id; metadata; data }
    ;;

    let to_v4 (t : 'a t @ m) ~(callee_menu : Menu.t option) : 'a V4.t @ m =
      (let { tag; version; id; metadata; data } = t in
       let rank =
         match callee_menu with
         | None -> None
         | Some callee_menu ->
           Menu.index__local callee_menu ~tag:(Rpc_tag.to_string_local tag) ~version
       in
       let specifier =
         match rank with
         | None -> V4.Rpc_specifier.Tag_and_version (tag, version)
         | Some rank -> V4.Rpc_specifier.Rank rank
       in
       { specifier; id; metadata; data })
      [@exclave_if_stack a]
    ;;

    let of_v3 (v3_query : 'a V3.t @ m) : 'a t @ m =
      let { V3.tag; version; id; metadata; data } = v3_query in
      { tag; version; id; metadata; data } [@exclave_if_stack a]
    ;;

    let to_v3 (t : 'a t @ m) : 'a V3.t @ m =
      let { tag; version; id; metadata; data } = t in
      { tag; version; id; metadata; data } [@exclave_if_stack a]
    ;;

    let of_v2 (v2_query : 'a V2.t @ m) : 'a t @ m =
      (let { V2.tag; version; id; metadata; data } = v2_query in
       let metadata =
         (Base.Option.map [@mode m]) metadata ~f:(Rpc_metadata.V2.of_v1 [@mode m])
       in
       { tag; version; id; metadata; data })
      [@exclave_if_stack a]
    ;;]

    let to_v2 (t : 'a t) : 'a V2.t =
      let { tag; version; id; metadata; data } = t in
      { tag
      ; version
      ; id
      ; metadata = Base.Option.bind metadata ~f:Rpc_metadata.V2.to_v1
      ; data
      }
    ;;

    let of_v1 (v1_query : 'a V1.t) : 'a t =
      let { V1.tag; version; id; data } = v1_query in
      { tag; version; id; metadata = None; data }
    ;;

    let to_v1 (t : 'a t) : 'a V1.t =
      let { tag; version; id; data; metadata = _ } = t in
      { tag; version; id; data }
    ;;
  end
end

module Impl_menu_index = Nat0.Option

module Response = struct
  module V1 = struct
    type 'a needs_length =
      { id : Query_id.t
      ; data : 'a Rpc_result.t
      }
    [@@deriving bin_io ~localize, globalize, sexp_of]

    let%expect_test _ =
      print_endline [%bin_digest: unit needs_length];
      [%expect {| e08147cd47ea743ad47cbb4abcd9448d |}]
    ;;

    type 'a t = 'a needs_length [@@deriving bin_read]
  end

  module V2 = struct
    type 'a needs_length =
      { id : Query_id.t
      ; impl_menu_index : Impl_menu_index.t
      ; data : 'a Rpc_result.t
      }
    [@@deriving bin_io ~localize, globalize, sexp_of]

    let%expect_test _ =
      print_endline [%bin_digest: unit needs_length];
      [%expect {| b7047e562833e76722265f8bad6f3746 |}]
    ;;

    type 'a t = 'a needs_length [@@deriving bin_read]
  end
end

module Stream_query = struct
  type 'a needs_length =
    [ `Query of 'a
    | `Abort
    ]
  [@@deriving bin_io ~localize, sexp_of]

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
  [@@deriving bin_io ~localize, sexp_of]

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
  [@@deriving bin_io ~localize, sexp_of]

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
    | Query_v1 of 'a Query.V1.needs_length
    | Response_v1 of 'a Response.V1.needs_length
    | Query_v2 of 'a Query.V2.needs_length
    | Metadata of Connection_metadata.V1.t
    | Close_reason of Info_with_local_bin_io.t
    | Close_reason_duplicated of Info_with_local_bin_io.t
    | Metadata_v2 of Connection_metadata.V2.t
    | Response_v2 of 'a Response.V2.needs_length
    | Query_v3 of 'a Query.V3.needs_length
    | Close_started
    | Close_reason_v2 of Close_reason.Protocol.Binable.t
    | Query_v4 of 'a Query.V4.needs_length
  [@@deriving bin_io ~localize, globalize, sexp_of, variants]

  let%expect_test "Stable unless we purposefully add a new variant" =
    print_endline [%bin_digest: unit maybe_needs_length];
    [%expect {| e8e9e042f02e523140204a2b26159669 |}]
  ;;

  type 'a t = 'a maybe_needs_length [@@deriving bin_read, sexp_of]
  type nat0_t = Nat0.t maybe_needs_length [@@deriving bin_read, bin_write]

  let globalize_nat0_t =
    globalize_maybe_needs_length (fun (local_ (nat0 : Nat0.t)) -> nat0)
  ;;

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
    let query : unit Query.Validated.t =
      { tag = Rpc_tag.of_string "tag"
      ; version = 0
      ; id = Query_id.of_int_exn 1
      ; metadata =
          Some
            (Rpc_metadata.V2.singleton
               Rpc_metadata.V2.Key.default_for_legacy
               (Rpc_metadata.V2.Payload.of_string_maybe_truncate "metadata")
             [@alert "-legacy_query_metadata"])
      ; data = ()
      }
    in
    let metadata : Connection_metadata.V2.t = { identification = None; menu = None } in
    let close_reason = Core.Info.create_s [%message "Close reason"] in
    let user_close_reason = Core.Info.create_s [%message "User close reason"] in
    Variants_of_maybe_needs_length.iter
      ~heartbeat:(print (fun c -> c))
      ~query_v1:(print (fun c -> c (Query.Validated.to_v1 query)))
      ~response_v1:
        (print (fun c ->
           c ({ id = Query_id.of_int_exn 0; data = Ok () } : _ Response.V1.needs_length)))
      ~query_v2:(print (fun c -> c (Query.Validated.to_v2 query)))
      ~query_v3:(print (fun c -> c (Query.Validated.to_v3 query)))
      ~query_v4:(fun variant ->
        print (fun c -> c (Query.Validated.to_v4 query ~callee_menu:None)) variant;
        print
          (fun c ->
            c
              (Query.Validated.to_v4
                 query
                 ~callee_menu:
                   (Some
                      (Menu.of_supported_rpcs
                         ~rpc_shapes:`Unknown
                         [ { Description.name = "not_the_intended_rpc"; version = 0 }
                         ; { name = "not_the_intended_rpc"; version = 1 }
                         ; { name = "tag"; version = 0 }
                         ]))))
          variant)
      ~metadata:(print (fun c -> c (Connection_metadata.V2.to_v1 metadata)))
      ~close_reason:(print (fun c -> c close_reason))
      ~close_reason_duplicated:(print (fun c -> c close_reason))
      ~close_reason_v2:
        (print (fun c ->
           c
             (Close_reason.Protocol.binable_of_t
                (Close_reason.Protocol.create
                   ~kind:Unspecified
                   ~debug_info:close_reason
                   ~user_reason:user_close_reason
                   ()))))
      ~metadata_v2:(print (fun c -> c metadata))
      ~response_v2:
        (print (fun c ->
           c
             ({ id = Query_id.of_int_exn 0
              ; impl_menu_index = Nat0.Option.some (Nat0.of_int_exn 5)
              ; data = Ok ()
              }
              : _ Response.V2.needs_length)))
      ~close_started:(print (fun c -> c));
    [%expect
      {|
      Heartbeat: 0
      00000000  00                                                |.|
      Query_v1: 1
      00000000  01 03 74 61 67 00 01 00                           |..tag...|
      Response_v1: 2
      00000000  02 00 00 00                                       |....|
      Query_v2: 3
      00000000  03 03 74 61 67 00 01 01  08 6d 65 74 61 64 61 74  |..tag....metadat|
      00000010  61 00                                             |a.|
      Metadata: 4
      00000000  04 00 00                                          |...|
      Close_reason: 5
      00000000  05 03 00 0c 43 6c 6f 73  65 20 72 65 61 73 6f 6e  |....Close reason|
      Close_reason_duplicated: 6
      00000000  06 03 00 0c 43 6c 6f 73  65 20 72 65 61 73 6f 6e  |....Close reason|
      Metadata_v2: 7
      00000000  07 00 00                                          |...|
      Response_v2: 8
      00000000  08 00 01 05 00 00                                 |......|
      Query_v3: 9
      00000000  09 03 74 61 67 00 01 01  01 00 08 6d 65 74 61 64  |..tag......metad|
      00000010  61 74 61 00                                       |ata.|
      Close_started: 10
      00000000  0a                                                |.|
      Close_reason_v2: 11
      00000000  0b 00 0b 55 6e 73 70 65  63 69 66 69 65 64 01 03  |...Unspecified..|
      00000010  00 0c 43 6c 6f 73 65 20  72 65 61 73 6f 6e 01 03  |..Close reason..|
      00000020  00 11 55 73 65 72 20 63  6c 6f 73 65 20 72 65 61  |..User close rea|
      00000030  73 6f 6e                                          |son|
      Query_v4: 12
      00000000  0c 00 03 74 61 67 00 01  01 01 00 08 6d 65 74 61  |...tag......meta|
      00000010  64 61 74 61 00                                    |data.|
      Query_v4: 12
      00000000  0c 01 02 01 01 01 00 08  6d 65 74 61 64 61 74 61  |........metadata|
      00000010  00                                                |.|
      |}]
  ;;
end
