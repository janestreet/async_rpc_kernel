(** Async_rpc protocol types, for internal use only *)

(* WARNING: do not change any of these types without good reason *)

open Bin_prot.Std
open Sexplib.Std
module Core_for_testing = Core
module Rpc_tag : Core.Identifiable = Core.String
module Query_id = Core.Unique_id.Int63 (struct end)

module Unused_query_id : sig
  type t [@@deriving bin_io, sexp_of]

  val t : t
end = struct
  type t = Query_id.t [@@deriving bin_io, sexp_of]

  let%expect_test _ =
    print_endline [%bin_digest: t];
    [%expect {| 2b528f4b22f08e28876ffe0239315ac2 |}]
  ;;

  let t = Query_id.create ()
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
  [@@deriving bin_io, sexp, compare]

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
    [@@deriving bin_io, sexp, compare]

    let%expect_test "stable" =
      print_endline [%bin_digest: t];
      [%expect {| 8cc766befa2cf565ea147d9fcd5eaaab |}]
    ;;
  end

  include T
  include Core.Comparable.Make (T)
end

module Rpc_result = struct
  type 'a t = ('a, Rpc_error.t) Core.Result.t [@@deriving bin_io, sexp_of]

  let%expect_test _ =
    print_endline [%bin_digest: unit t];
    [%expect {| 58734a63a5c83c1b7cbfc3fedfa3ae82 |}]
  ;;
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
    [%expect {| a22a76f192b3a4ec1c37117c6bb252f5 |}]
  ;;

  type 'a t = 'a needs_length [@@deriving bin_read]
end

module Stream_query = struct
  type 'a needs_length =
    [ `Query of 'a
    | `Abort
    ]
  [@@deriving bin_io]

  let%expect_test _ =
    print_endline [%bin_digest: unit needs_length];
    [%expect {| 2c37868761971c78cc355d43f0854860 |}]
  ;;

  type 'a t = 'a needs_length [@@deriving bin_read]
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
  [@@deriving bin_io]

  let%expect_test _ =
    print_endline [%bin_digest: unit needs_length];
    [%expect {| c1dbcdcfe2b12e797ec64f0d74df1811 |}]
  ;;

  type 'a t = 'a needs_length [@@deriving bin_read]
  type nat0_t = Nat0.t needs_length [@@deriving bin_read, bin_write]
end

module Message = struct
  type 'a needs_length =
    | Heartbeat
    | Query_v1 of 'a Query_v1.needs_length
    | Response of 'a Response.needs_length
    | Query of 'a Query.needs_length
  [@@deriving bin_io, sexp_of]

  let%expect_test _ =
    print_endline [%bin_digest: unit needs_length];
    [%expect {| f60ce2d104e2f0d9271f622ecd97cea8 |}]
  ;;

  type 'a t = 'a needs_length [@@deriving bin_read, sexp_of]
  type nat0_t = Nat0.t needs_length [@@deriving bin_read, bin_write]
end

let%test "v1 message compatibility" =
  let module Message_v1 = struct
    type 'a needs_length =
      | Heartbeat
      | Query of 'a Query_v1.needs_length
      | Response of 'a Response.needs_length
    [@@deriving bin_io, sexp_of]

    let%expect_test _ =
      print_endline [%bin_digest: unit needs_length];
      [%expect {| 14965b0db9844e6b376151dd890808e8 |}]
    ;;
  end
  in
  let tag = Rpc_tag.of_string "rpc" in
  let query_id = Query_id.create () in
  let v1_query = { Query_v1.tag; version = 1; id = query_id; data = () } in
  let response = { Response.id = query_id; data = Ok () } in
  let v1 = Message_v1.[ Heartbeat; Query v1_query; Response response ] in
  let v2 = Message.[ Heartbeat; Query_v1 v1_query; Response response ] in
  let v1_str =
    Core_for_testing.Bin_prot.Writer.to_string
      [%bin_writer: unit Message_v1.needs_length list]
      v1
  in
  let v2_str =
    Core_for_testing.Bin_prot.Writer.to_string
      [%bin_writer: unit Message.needs_length list]
      v2
  in
  String.equal v1_str v2_str
;;
