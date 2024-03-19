open Core
open Async_kernel
open Protocol

(** The types of the [Implementation] and [Implementations] modules, which have
    a dependency cyle: [Implementation] -> [Direct_stream_writer] ->
    [Implementations] -> [Implementation]. *)

module Direct_stream_writer_id = Unique_id.Int63 ()

module On_exception = struct
  type t =
    { callback : (exn -> unit) option [@sexp.omit_nil]
    ; close_connection_if_no_return_value : bool
    }
  [@@deriving sexp_of]
end

module rec Implementation : sig
  module Expert : sig
    module Responder : sig
      type t =
        { query_id : Query_id.t
        ; writer : Protocol_writer.t
        ; mutable responded : bool
        }
    end

    type implementation_result =
      | Replied
      | Delayed_response of unit Deferred.t
  end

  module F : sig
    type _ error_mode =
      | Always_ok : _ error_mode
      | Using_result : (_, _) Result.t error_mode
      | Using_result_result : ((_, _) Result.t, _) Result.t error_mode
      | Is_error : ('a -> bool) -> 'a error_mode
      | Streaming_initial_message : (_, _) Protocol.Stream_initial_message.t error_mode

    type (_, _) result_mode =
      | Blocking : ('a, 'a Or_not_authorized.t) result_mode
      | Deferred : ('a, 'a Or_not_authorized.t Deferred.t) result_mode

    type ('connection_state, 'query, 'init, 'update) streaming_impl =
      | Pipe of
          ('connection_state
           -> 'query
           -> ('init * 'update Pipe.Reader.t, 'init) Result.t Or_not_authorized.t
              Deferred.t)
      | Direct of
          ('connection_state
           -> 'query
           -> 'update Direct_stream_writer.t
           -> ('init, 'init) Result.t Or_not_authorized.t Deferred.t)

    type ('connection_state, 'query, 'init, 'update) streaming_rpc =
      { bin_query_reader : 'query Bin_prot.Type_class.reader
      ; bin_init_writer : 'init Bin_prot.Type_class.writer
      ; bin_update_writer : 'update Bin_prot.Type_class.writer
          (* 'init can be an error or an initial state *)
      ; impl : ('connection_state, 'query, 'init, 'update) streaming_impl
      ; error_mode : 'init error_mode
      }

    type 'connection_state t =
      | One_way :
          'msg Bin_prot.Type_class.reader
          * ('connection_state -> 'msg -> unit Or_not_authorized.t Deferred.t)
          -> 'connection_state t
      | One_way_expert :
          ('connection_state
           -> Bigstring.t
           -> pos:int
           -> len:int
           -> unit Or_not_authorized.t Deferred.t)
          -> 'connection_state t
      | Rpc :
          'query Bin_prot.Type_class.reader
          * 'response Bin_prot.Type_class.writer
          * ('connection_state -> 'query -> 'result)
          * 'response error_mode
          * ('response, 'result) result_mode
          -> 'connection_state t
      | Rpc_expert :
          ('connection_state
           -> Expert.Responder.t
           -> Bigstring.t
           -> pos:int
           -> len:int
           -> 'result)
          * (Expert.implementation_result, 'result) result_mode
          -> 'connection_state t
      | Streaming_rpc :
          ('connection_state, 'query, 'init, 'update) streaming_rpc
          -> 'connection_state t
      (* [Legacy_menu_rpc] is a hack in order to allow us to share the versioned menu in
         connection metadata. Old clients still require the [__Versioned_rpc.Menu] rpc to
         exist and the menu sent in the metadata must match whatever existed beforehand
         when calling [Menu.add] so we created a custom implementation type that is used
         in [Menu.add] that lets us dispatch the rpc without a ['connection_state].
         Externally this looks and acts like a [Menu.Stable.V1.response]-returning server.
      *)
      | Legacy_menu_rpc : Menu.Stable.V2.response Lazy.t -> 'connection_state t
  end

  type 'connection_state t =
    { tag : Rpc_tag.t
    ; version : int
    ; f : 'connection_state F.t
    ; shapes : (Rpc_shapes.t * Rpc_shapes.Just_digests.t) Lazy.t
    ; on_exception : On_exception.t
    }
end =
  Implementation

and Implementations : sig
  type 'connection_state on_unknown_rpc =
    [ `Raise
    | `Continue
    | `Close_connection
    | `Call of
      'connection_state
      -> rpc_tag:string
      -> version:int
      -> [ `Close_connection | `Continue ]
    | `Expert of
      'connection_state
      -> rpc_tag:string
      -> version:int
      -> metadata:string option
      -> Implementation.Expert.Responder.t
      -> Bigstring.t
      -> pos:int
      -> len:int
      -> unit Deferred.t
    ]

  type 'connection_state t =
    { implementations : 'connection_state Implementation.t Description.Table.t
    ; on_unknown_rpc : 'connection_state on_unknown_rpc
    }

  type 'connection_state implementations = 'connection_state t

  module rec Instance : sig
    type streaming_response =
      | Pipe : _ Pipe.Reader.t -> streaming_response
      | Direct : _ Direct_stream_writer.t -> streaming_response

    type 'a unpacked =
      { implementations : 'a implementations
      ; writer : Protocol_writer.t
      ; events : (Tracing_event.t -> unit) Bus.Read_write.t
      ; open_streaming_responses : (Query_id.t, streaming_response) Hashtbl.t
      ; mutable stopped : bool
      ; connection_state : 'a
      ; connection_description : Info.t
      ; connection_close_started : Info.t Deferred.t
      ; mutable
          last_dispatched_implementation :
          (Description.t * 'a Implementation.t) option
      ; mutable
          on_receive :
          Description.t
          -> query_id:Query_id.t
          -> string option
          -> Execution_context.t
          -> Execution_context.t
      }

    type t = T : _ unpacked -> t [@@unboxed]
  end
end =
  Implementations

and Direct_stream_writer : sig
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
    ; closed : unit Ivar.t
    ; instance : Implementations.Instance.t
    ; query_id : Query_id.t
    ; rpc : Description.t
    ; stream_writer : 'a Cached_bin_writer.t
    ; groups : 'a group_entry Bag.t
    }

  and 'a group_entry =
    { group : 'a Direct_stream_writer.Group.t
    ; element_in_group : 'a t Bag.Elt.t
    }

  module Group : sig
    type 'a direct_stream_writer = 'a t

    type 'a t =
      { (* [components] is only tracked separately from [components_by_id] so we can iterate
           over its elements more quickly than we could iterate over the values of
           [components_by_id]. *)
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

and Cached_bin_writer : sig
  type 'a t =
    { header_prefix : string (* Bin_protted constant prefix of the message *)
    ; mutable data_len : Nat0.t
    ; bin_writer : 'a Bin_prot.Type_class.writer
    }
end =
  Cached_bin_writer
