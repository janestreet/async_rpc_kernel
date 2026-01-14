open Core
open Async_kernel
open Protocol

(** The types of the [Implementation] and [Implementations] modules, which have a
    dependency cyle: [Implementation] -> [Direct_stream_writer] -> [Implementations] ->
    [Implementation]. *)

module Direct_stream_writer_id = Unique_id.Int63 ()

module rec Direct_stream_writer : sig
  module Pending_response : sig
    type 'a t =
      | Normal of 'a
      | Expert_string of string
      | Expert_schedule_bigstring of
          { buf : Bigstring.t
          ; pos : int
          ; len : int
          ; done_ : unit Ivar.t
          }
  end

  module State : sig
    type 'a t =
      | Not_started of 'a Pending_response.t Queue.t
      | Started
  end

  module Id = Direct_stream_writer_id

  type 'a t =
    { id : Id.t
    ; mutable state : 'a State.t
    ; started : unit Ivar.t
    ; closed : unit Ivar.t
    ; query_id : Query_id.t
    ; rpc : Description.t
    ; stream_writer : 'a Cached_streaming_response_writer.t
    ; groups : 'a group_entry Bag.t
    ; mutable instance_stopped : bool
    ; cleanup_and_write_streaming_eof : [ `Eof ] Rpc_result.t -> unit
    }

  and 'a group_entry =
    { group : 'a Direct_stream_writer.Group.t
    ; element_in_group : 'a t Bag.Elt.t
    }

  module Group : sig
      type 'a direct_stream_writer = 'a t

      type 'a t =
        { (* [components] is only tracked separately from [components_by_id] so we can
             iterate over its elements more quickly than we could iterate over the values
             of [components_by_id]. *)
          mutable components : 'a direct_stream_writer Bag.t
        ; components_by_id : 'a component Id.Table.t
        ; buffer : Bigstring.t ref
        ; mutable last_value_len : int
        ; last_value_not_written : 'a Moption.t
        ; send_last_value_on_add : bool
        }

      and 'a component =
        { writer_element_in_group : 'a direct_stream_writer Bag.Elt.t
        ; group_element_in_writer : 'a group_entry Bag.Elt.t
        }
    end
    with type 'a direct_stream_writer := 'a t
end =
  Direct_stream_writer
