open! Core
open Bin_prot.Read
include Protocol

module Sexp = struct
  include Sexp

  let rec bin_read_t__local buf ~pos_ref =
    match Bin_prot.Read.bin_read_int_8bit buf ~pos_ref with
    | 0 ->
      exclave_
      let atom = bin_read_string__local buf ~pos_ref in
      Atom atom
    | 1 ->
      exclave_
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

  let[@zero_alloc opt] bin_read_t__local buf ~pos_ref = exclave_
    bin_read_string__local buf ~pos_ref |> Rpc_tag.of_string_local
  ;;
end

module Query_id = struct
  include Query_id

  let[@zero_alloc opt] bin_read_t__local buf ~pos_ref = bin_read_t buf ~pos_ref
end

module Unused_query_id = struct
  include Unused_query_id

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
      exclave_
      let data = Sexp.bin_read_t__local buf ~pos_ref in
      Bin_io_exn data
    | 1 -> exclave_ Connection_closed
    | 2 ->
      exclave_
      let arg = Sexp.bin_read_t__local buf ~pos_ref in
      Write_error arg
    | 3 ->
      exclave_
      let arg = Sexp.bin_read_t__local buf ~pos_ref in
      Uncaught_exn arg
    | 4 ->
      exclave_
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
      exclave_
      let query_id = Query_id.bin_read_t__local buf ~pos_ref in
      Unknown_query_id query_id
    | 6 ->
      exclave_
      let arg = Sexp.bin_read_t__local buf ~pos_ref in
      Authorization_failure arg
    | 7 ->
      exclave_
      let message_too_big =
        Transport.Send_result.bin_read_message_too_big__local buf ~pos_ref
      in
      Message_too_big message_too_big
    | 8 ->
      exclave_
      let arg = Sexp.bin_read_t__local buf ~pos_ref in
      Unknown arg
    | 9 ->
      exclave_
      let arg = Sexp.bin_read_t__local buf ~pos_ref in
      Lift_error arg
    | _ ->
      Bin_prot.Common.raise_read_error
        (Bin_prot.Common.ReadError.Sum_tag "Rpc_error local reader")
        !pos_ref
  ;;
end

module Result = struct
  include Result

  let[@zero_alloc opt] bin_read_t__local bin_read_ok bin_read_error buf ~pos_ref =
    match Bin_prot.Read.bin_read_int_8bit buf ~pos_ref with
    | 0 ->
      exclave_
      let data = (bin_read_ok [@zero_alloc assume]) buf ~pos_ref in
      Ok data
    | 1 ->
      exclave_
      let err = (bin_read_error [@zero_alloc assume]) buf ~pos_ref in
      Error err
    | _ ->
      Bin_prot.Common.raise_read_error
        (Bin_prot.Common.ReadError.Sum_tag "Rpc_result local reader")
        !pos_ref
  ;;
end

module Rpc_result = struct
  include Rpc_result

  type nonrec 'a t = ('a, Rpc_error.t) Core.Result.t

  let[@zero_alloc opt] bin_read_t__local bin_read_el buf ~pos_ref = exclave_
    Result.bin_read_t__local bin_read_el Rpc_error.bin_read_t__local buf ~pos_ref
  ;;
end

module Query = struct
  include Query

  module V1 = struct
    include V1

    type nonrec 'a needs_length = 'a needs_length =
      { tag : Rpc_tag.t
      ; version : int
      ; id : Query_id.t
      ; data : 'a
      }

    let bin_read_t__local bin_read_el buf ~pos_ref = exclave_
      let tag = Rpc_tag.bin_read_t__local buf ~pos_ref in
      let version = bin_read_int buf ~pos_ref in
      let id = Query_id.bin_read_t__local buf ~pos_ref in
      let data = bin_read_el buf ~pos_ref in
      { tag; version; id; data }
    ;;
  end

  module V2 = struct
    include V2

    type nonrec 'a needs_length = 'a needs_length =
      { tag : Rpc_tag.t
      ; version : int
      ; id : Query_id.t
      ; metadata : Rpc_metadata.V1.t option
      ; data : 'a
      }

    let bin_read_t__local bin_read_el buf ~pos_ref = exclave_
      let tag = Rpc_tag.bin_read_t__local buf ~pos_ref in
      let version = bin_read_int buf ~pos_ref in
      let id = Query_id.bin_read_t__local buf ~pos_ref in
      let metadata =
        bin_read_option__local Rpc_metadata.V1.bin_read_t__local buf ~pos_ref
      in
      let data = bin_read_el buf ~pos_ref in
      { tag; version; id; metadata; data }
    ;;
  end

  module V3 = struct
    include V3

    type nonrec 'a needs_length = 'a needs_length =
      { tag : Rpc_tag.t
      ; version : int
      ; id : Query_id.t
      ; metadata : Rpc_metadata.V2.t option
      ; data : 'a
      }

    let bin_read_t__local bin_read_el buf ~pos_ref = exclave_
      let tag = Rpc_tag.bin_read_t__local buf ~pos_ref in
      let version = bin_read_int buf ~pos_ref in
      let id = Query_id.bin_read_t__local buf ~pos_ref in
      let metadata =
        bin_read_option__local Rpc_metadata.V2.bin_read_t__local buf ~pos_ref
      in
      let data = bin_read_el buf ~pos_ref in
      { tag; version; id; metadata; data }
    ;;
  end

  module V4 = struct
    include V4

    module Rpc_specifier = struct
      include Rpc_specifier

      type nonrec t = t =
        | Tag_and_version of Rpc_tag.t * int
        | Rank of int

      let bin_read_t__local buf ~pos_ref =
        match Bin_prot.Read.bin_read_int_8bit buf ~pos_ref with
        | 0 ->
          exclave_
          let tag = Rpc_tag.bin_read_t__local buf ~pos_ref in
          let version = Bin_prot.Read.bin_read_int buf ~pos_ref in
          Rpc_specifier.Tag_and_version (tag, version)
        | 1 ->
          exclave_
          let rank = Bin_prot.Read.bin_read_int buf ~pos_ref in
          Rpc_specifier.Rank rank
        | n ->
          Bin_prot.Common.raise_variant_wrong_type "Protocol.Query.V4.Rpc_specifier" n
      ;;
    end

    type nonrec 'a needs_length = 'a needs_length =
      { specifier : Rpc_specifier.t
      ; id : Query_id.t
      ; metadata : Rpc_metadata.V2.t option
      ; data : 'a
      }

    let bin_read_t__local bin_read_el buf ~pos_ref = exclave_
      let specifier = Rpc_specifier.bin_read_t__local buf ~pos_ref in
      let id = Query_id.bin_read_t__local buf ~pos_ref in
      let metadata =
        bin_read_option__local Rpc_metadata.V2.bin_read_t__local buf ~pos_ref
      in
      let data = bin_read_el buf ~pos_ref in
      { specifier; id; metadata; data }
    ;;
  end
end

module Response = struct
  include Response

  module V1 = struct
    include V1

    type nonrec 'a needs_length = 'a needs_length =
      { id : Query_id.t
      ; data : 'a Rpc_result.t
      }

    let bin_read_t__local bin_read_el buf ~pos_ref = exclave_
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

    let bin_read_t__local bin_read_el buf ~pos_ref = exclave_
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

    let bin_read_t__local buf ~pos_ref = exclave_
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

    let bin_read_t__local buf ~pos_ref = exclave_
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
    | Query_v1 of 'a Query.V1.needs_length
    | Response_v1 of 'a Response.V1.needs_length
    | Query_v2 of 'a Query.V2.needs_length
    | Metadata of Connection_metadata.V1.t
    | Close_reason of Info.t
    | Close_reason_duplicated of Info.t
    | Metadata_v2 of Connection_metadata.V2.t
    | Response_v2 of 'a Response.V2.needs_length
    | Query_v3 of 'a Query.V3.needs_length
    | Close_started
    | Close_reason_v2 of Close_reason.Protocol.Binable.t
    | Query_v4 of 'a Query.V4.needs_length

  let bin_read_t__local : ('a, 'a t) reader1__local =
    fun bin_read_el buf ~pos_ref ->
    match Bin_prot.Read.bin_read_int_8bit buf ~pos_ref with
    | 0 -> exclave_ Heartbeat
    | 1 ->
      exclave_
      let query = Query.V1.bin_read_t__local bin_read_el buf ~pos_ref in
      Query_v1 query
    | 2 ->
      exclave_
      let response = Response.V1.bin_read_t__local bin_read_el buf ~pos_ref in
      Response_v1 response
    | 3 ->
      exclave_
      let query = Query.V2.bin_read_t__local bin_read_el buf ~pos_ref in
      Query_v2 query
    | 4 ->
      exclave_
      let metadata = Connection_metadata.V1.bin_read_t__local buf ~pos_ref in
      Metadata metadata
    | 5 ->
      exclave_
      let close_reason = Info.bin_read_t buf ~pos_ref in
      Close_reason ([%globalize: Core.Info.t] close_reason)
    | 6 ->
      exclave_
      let close_reason = Info.bin_read_t buf ~pos_ref in
      Close_reason_duplicated ([%globalize: Core.Info.t] close_reason)
    | 7 ->
      exclave_
      let metadata = Connection_metadata.V2.bin_read_t__local buf ~pos_ref in
      Metadata_v2 metadata
    | 8 ->
      exclave_
      let response = Response.V2.bin_read_t__local bin_read_el buf ~pos_ref in
      Response_v2 response
    | 9 ->
      exclave_
      let query = Query.V3.bin_read_t__local bin_read_el buf ~pos_ref in
      Query_v3 query
    | 10 -> exclave_ Close_started
    | 11 ->
      exclave_
      let close_reason = Close_reason.Protocol.Binable.bin_read_t buf ~pos_ref in
      Close_reason_v2 close_reason
    | 12 ->
      exclave_
      let query = Query.V4.bin_read_t__local bin_read_el buf ~pos_ref in
      Query_v4 query
    | _ ->
      Bin_prot.Common.raise_read_error
        (Bin_prot.Common.ReadError.Sum_tag "Message local reader")
        !pos_ref
  ;;

  let bin_read_nat0_t__local = bin_read_t__local Nat0.bin_read_t
end

module Stream_query = struct
  include Stream_query

  let bin_read_needs_length__local : ('a, 'a needs_length) reader1__local =
    fun bin_read_el buf ~pos_ref ->
    match Bin_prot.Read.bin_read_variant_int__local buf ~pos_ref with
    | -250086680 ->
      exclave_
      let el = bin_read_el buf ~pos_ref in
      `Query el
    | 774323088 -> exclave_ `Abort
    | _ ->
      Bin_prot.Common.raise_read_error
        (Bin_prot.Common.ReadError.Sum_tag "Stream_query local reader")
        !pos_ref
  ;;

  let bin_read_nat0_t__local = bin_read_needs_length__local Nat0.bin_read_t
end

module Stream_initial_message = struct
  include Stream_initial_message

  let bin_read_t__local bin_read_response bin_read_error buf ~pos_ref = exclave_
    let unused_query_id = Unused_query_id.bin_read_t__local buf ~pos_ref in
    let initial =
      (Result.bin_read_t__local bin_read_response bin_read_error) buf ~pos_ref
    in
    { unused_query_id; initial }
  ;;
end

module Stream_response_data = struct
  include Stream_response_data

  let bin_read_needs_length__local : ('a, 'a needs_length) reader1__local =
    fun bin_read_el buf ~pos_ref ->
    match Bin_prot.Read.bin_read_variant_int__local buf ~pos_ref with
    | 17724 ->
      exclave_
      let el = bin_read_el buf ~pos_ref in
      `Ok el
    | 3456156 -> `Eof
    | _ ->
      Bin_prot.Common.raise_read_error
        (Bin_prot.Common.ReadError.Sum_tag "Stream_response_data local reader")
        !pos_ref
  ;;

  let bin_read_nat0_t__local = bin_read_needs_length__local Nat0.bin_read_t
end
