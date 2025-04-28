open! Core
open Bin_prot.Read
include Protocol

module Sexp = struct
  include Sexp

  let rec bin_read_t__local buf ~pos_ref =
    match Bin_prot.Read.bin_read_int_8bit buf ~pos_ref with
    | 0 ->
      let atom = bin_read_string__local buf ~pos_ref in
      Atom atom
    | 1 ->
      let list = bin_read_list__local bin_read_t__local buf ~pos_ref in
      List list
    | _ ->
      Bin_prot.Common.raise_read_error
        (Bin_prot.Common.ReadError.Sum_tag "Sexp local reader")
        !pos_ref
  ;;
end

module Rpc_tag = struct
  include Rpc_tag

  let[@zero_alloc opt] bin_read_t__local buf ~pos_ref =
    bin_read_string__local buf ~pos_ref |> Rpc_tag.of_string_local
  ;;
end

module Query_id = struct
  include Query_id

  let[@zero_alloc opt] bin_read_t__local buf ~pos_ref = bin_read_t buf ~pos_ref
end

module Rpc_error = struct
  include Rpc_error

  type nonrec t = t =
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

  let[@zero_alloc opt] bin_read_t__local buf ~pos_ref =
    match Bin_prot.Read.bin_read_int_8bit buf ~pos_ref with
    | 0 ->
      let data = Sexp.bin_read_t__local buf ~pos_ref in
      Bin_io_exn data
    | 1 -> Connection_closed
    | 2 ->
      let arg = Sexp.bin_read_t__local buf ~pos_ref in
      Write_error arg
    | 3 ->
      let arg = Sexp.bin_read_t__local buf ~pos_ref in
      Uncaught_exn arg
    | 4 ->
      let tag = Rpc_tag.bin_read_t__local buf ~pos_ref in
      let version =
        let vint = Bin_prot.Read.bin_read_variant_int buf ~pos_ref in
        match vint with
        | -901574920 ->
          let arg_1 = bin_read_int buf ~pos_ref in
          `Version arg_1
        | _ -> Bin_prot.Common.raise_variant_wrong_type "Rpc_error local reader" !pos_ref
      in
      Unimplemented_rpc (tag, version)
    | 5 ->
      let query_id = Query_id.bin_read_t__local buf ~pos_ref in
      Unknown_query_id query_id
    | 6 ->
      let arg = Sexp.bin_read_t__local buf ~pos_ref in
      Authorization_failure arg
    | 7 ->
      let message_too_big =
        Transport.Send_result.bin_read_message_too_big__local buf ~pos_ref
      in
      Message_too_big message_too_big
    | 8 ->
      let arg = Sexp.bin_read_t__local buf ~pos_ref in
      Unknown arg
    | 9 ->
      let arg = Sexp.bin_read_t__local buf ~pos_ref in
      Lift_error arg
    | _ ->
      Bin_prot.Common.raise_read_error
        (Bin_prot.Common.ReadError.Sum_tag "Rpc_error local reader")
        !pos_ref
  ;;
end

module Rpc_result = struct
  include Rpc_result

  type nonrec 'a t = ('a, Rpc_error.t) Core.Result.t

  let bin_read_t__local bin_read_el buf ~pos_ref =
    match Bin_prot.Read.bin_read_int_8bit buf ~pos_ref with
    | 0 ->
      let data = bin_read_el buf ~pos_ref in
      Ok data
    | 1 ->
      let err = Rpc_error.bin_read_t__local buf ~pos_ref in
      Error err
    | _ ->
      Bin_prot.Common.raise_read_error
        (Bin_prot.Common.ReadError.Sum_tag "Rpc_result local reader")
        !pos_ref
  ;;
end

module Query_v1 = struct
  include Query_v1

  type nonrec 'a needs_length = 'a needs_length =
    { tag : Rpc_tag.t
    ; version : int
    ; id : Query_id.t
    ; data : 'a
    }

  let bin_read_t__local bin_read_el buf ~pos_ref =
    let tag = Rpc_tag.bin_read_t__local buf ~pos_ref in
    let version = bin_read_int buf ~pos_ref in
    let id = Query_id.bin_read_t__local buf ~pos_ref in
    let data = bin_read_el buf ~pos_ref in
    { tag; version; id; data }
  ;;
end

module Query = struct
  include Query

  type nonrec 'a needs_length = 'a needs_length =
    { tag : Rpc_tag.t
    ; version : int
    ; id : Query_id.t
    ; metadata : string option
    ; data : 'a
    }

  let bin_read_t__local bin_read_el buf ~pos_ref =
    let tag = Rpc_tag.bin_read_t__local buf ~pos_ref in
    let version = bin_read_int buf ~pos_ref in
    let id = Query_id.bin_read_t__local buf ~pos_ref in
    let metadata = bin_read_option__local bin_read_string__local buf ~pos_ref in
    let data = bin_read_el buf ~pos_ref in
    { tag; version; id; metadata; data }
  ;;
end

module Response = struct
  include Response

  module V1 = struct
    include V1

    type nonrec 'a needs_length = 'a needs_length =
      { id : Query_id.t
      ; data : 'a Rpc_result.t
      }

    let bin_read_t__local bin_read_el buf ~pos_ref =
      let id = Query_id.bin_read_t__local buf ~pos_ref in
      let data = Rpc_result.bin_read_t__local bin_read_el buf ~pos_ref in
      { id; data }
    ;;
  end

  module V2 = struct
    include V2

    type nonrec 'a needs_length = 'a needs_length =
      { id : Query_id.t
      ; impl_menu_index : Nat0.Option.t
      ; data : 'a Rpc_result.t
      }

    let bin_read_t__local bin_read_el buf ~pos_ref =
      let id = Query_id.bin_read_t__local buf ~pos_ref in
      let impl_menu_index = Nat0.Option.bin_read_t__local buf ~pos_ref in
      let data = Rpc_result.bin_read_t__local bin_read_el buf ~pos_ref in
      { id; impl_menu_index; data }
    ;;
  end
end

module Connection_metadata = struct
  include Connection_metadata

  module V1 = struct
    include V1

    type nonrec t = t =
      { identification : Core.Bigstring.Stable.V1.t option
      ; menu : Menu.Stable.V2.response option
      }

    let bin_read_t__local buf ~pos_ref =
      let identification =
        bin_read_option__local bin_read_bigstring__local buf ~pos_ref
      in
      let menu =
        bin_read_option__local Menu.Stable.V2.bin_read_response__local buf ~pos_ref
      in
      { identification; menu }
    ;;
  end

  module V2 = struct
    include V2

    type nonrec t = t =
      { identification : Core.Bigstring.Stable.V1.t option
      ; menu : Menu.Stable.V3.response option
      }

    let bin_read_t__local buf ~pos_ref =
      let identification =
        bin_read_option__local bin_read_bigstring__local buf ~pos_ref
      in
      let menu =
        bin_read_option__local Menu.Stable.V3.bin_read_response__local buf ~pos_ref
      in
      { identification; menu }
    ;;
  end
end

module Message = struct
  include Message

  type nonrec 'a maybe_needs_length = 'a maybe_needs_length =
    | Heartbeat
    | Query_v1 of 'a Query_v1.needs_length
    | Response_v1 of 'a Response.V1.needs_length
    | Query of 'a Query.needs_length
    | Metadata of Connection_metadata.V1.t
    | Close_reason of Info.t
    | Close_reason_duplicated of Info.t
    | Metadata_v2 of Connection_metadata.V2.t
    | Response_v2 of 'a Response.V2.needs_length

  let bin_read_t__local : ('a, 'a t) reader1__local =
    fun bin_read_el buf ~pos_ref ->
    match Bin_prot.Read.bin_read_int_8bit buf ~pos_ref with
    | 0 -> Heartbeat
    | 1 ->
      let query = Query_v1.bin_read_t__local bin_read_el buf ~pos_ref in
      Query_v1 query
    | 2 ->
      let response = Response.V1.bin_read_t__local bin_read_el buf ~pos_ref in
      Response_v1 response
    | 3 ->
      let query = Query.bin_read_t__local bin_read_el buf ~pos_ref in
      Query query
    | 4 ->
      let metadata = Connection_metadata.V1.bin_read_t__local buf ~pos_ref in
      Metadata metadata
    | 5 ->
      let close_reason = Info.bin_read_t buf ~pos_ref in
      Close_reason ([%globalize: Core.Info.t] close_reason)
    | 6 ->
      let close_reason = Info.bin_read_t buf ~pos_ref in
      Close_reason_duplicated ([%globalize: Core.Info.t] close_reason)
    | 7 ->
      let metadata = Connection_metadata.V2.bin_read_t__local buf ~pos_ref in
      Metadata_v2 metadata
    | 8 ->
      let response = Response.V2.bin_read_t__local bin_read_el buf ~pos_ref in
      Response_v2 response
    | _ ->
      Bin_prot.Common.raise_read_error
        (Bin_prot.Common.ReadError.Sum_tag "Message local reader")
        !pos_ref
  ;;

  let bin_read_nat0_t__local = bin_read_t__local Nat0.bin_read_t
end
