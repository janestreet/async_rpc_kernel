open Core_kernel.Std
open Async_kernel.Std
open Protocol

(** The types of the [Implementation] and [Implementations] modules, which have
    a dependency cyle: [Implementation] -> [Direct_stream_writer] ->
    [Implementations] -> [Implementation]. *)

module rec Implementation : sig
  module Expert : sig
    module Responder : sig
      type t =
        { query_id          : Query_id.t
        ; writer            : Transport.Writer.t
        ; mutable responded : bool
        }
    end

    type implementation_result =
      | Replied
      | Delayed_response of unit Deferred.t
  end

  module F : sig
    type (_, _) result_mode =
      | Blocking : ('a, 'a           ) result_mode
      | Deferred : ('a, 'a Deferred.t) result_mode

    type ('connection_state, 'query, 'init, 'update) streaming_impl =
      | Pipe of
          ('connection_state
           -> 'query
           -> ('init * 'update Pipe.Reader.t, 'init) Result.t Deferred.t
          )
      | Direct of
          ('connection_state
           -> 'query
           -> 'update Direct_stream_writer.t
           -> ('init, 'init) Result.t Deferred.t
          )

    type 'connection_state t =
      | One_way
        : 'msg Bin_prot.Type_class.reader
          * ('connection_state -> 'msg -> unit )
        -> 'connection_state t
      | One_way_expert
        : ('connection_state -> Bigstring.t -> pos : int -> len : int -> unit)
        -> 'connection_state t
      | Rpc
        : 'query Bin_prot.Type_class.reader
          * 'response Bin_prot.Type_class.writer
          * ('connection_state -> 'query -> 'result)
          * ('response, 'result) result_mode
        -> 'connection_state t
      | Rpc_expert
        : ('connection_state
           -> Expert.Responder.t
           -> Bigstring.t
           -> pos : int
           -> len : int
           -> 'result)
          * (Expert.implementation_result, 'result) result_mode
        -> 'connection_state t
      | Streaming_rpc
        : 'query Bin_prot.Type_class.reader
          * 'init Bin_prot.Type_class.writer
          * 'update Bin_prot.Type_class.writer
          * ('connection_state, 'query, 'init, 'update) streaming_impl
        -> 'connection_state t
  end

  type 'connection_state t =
    { tag     : Rpc_tag.t
    ; version : int
    ; f       : 'connection_state F.t
    }
end = Implementation

and Implementations : sig
  type 'connection_state on_unknown_rpc =
    [ `Raise
    | `Continue
    | `Close_connection
    | `Call of
        ('connection_state
         -> rpc_tag : string
         -> version : int
         -> [ `Close_connection | `Continue ])
    | `Expert of
        ('connection_state
         -> rpc_tag : string
         -> version : int
         -> Implementation.Expert.Responder.t
         -> Bigstring.t
         -> pos : int
         -> len : int
         -> unit Deferred.t)
    ]

  type 'connection_state t =
    { implementations : 'connection_state Implementation.F.t Description.Table.t
    ; on_unknown_rpc  : 'connection_state on_unknown_rpc
    }

  type 'connection_state implementations = 'connection_state t

  module rec Instance : sig
    type streaming_response =
      | Pipe : _ Pipe.Reader.t -> streaming_response
      | Direct : _ Direct_stream_writer.t -> streaming_response

    type 'a unpacked =
      { implementations          : 'a implementations
      ; writer                   : Transport.Writer.t
      ; open_streaming_responses : (Query_id.t, streaming_response) Hashtbl.t
      ; mutable stopped          : bool
      ; connection_state         : 'a
      ; connection_description   : Info.t
      ; mutable last_dispatched_implementation :
          (Description.t * 'a Implementation.F.t) option
      ; packed_self              : t
      }

    and t = T : _ unpacked -> t
  end
end = Implementations

and Direct_stream_writer : sig
  module State : sig
    type 'a t =
      | Not_started of 'a Queue.t
      | Started
  end

  type 'a t = {
    mutable state : 'a State.t;
    closed : unit Ivar.t;
    instance : Implementations.Instance.t;
    query_id : Query_id.t;
    bin_writer : 'a Bin_prot.Type_class.writer;
  }
end = Direct_stream_writer
