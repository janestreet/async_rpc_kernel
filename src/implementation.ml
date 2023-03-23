open Core
open Async_kernel
open Protocol
open Implementation_types.Implementation

module Expert = struct
  module Responder = struct
    type t = Expert.Responder.t =
      { query_id : Query_id.t
      ; writer : Transport.Writer.t
      ; mutable responded : bool
      }
    [@@deriving sexp_of]

    let create query_id writer = { query_id; writer; responded = false }
  end

  type implementation_result = Expert.implementation_result =
    | Replied
    | Delayed_response of unit Deferred.t
end

module F = struct
  type ('a, 'b) result_mode = ('a, 'b) F.result_mode =
    | Blocking : ('a, 'a Or_not_authorized.t) result_mode
    | Deferred : ('a, 'a Or_not_authorized.t Deferred.t) result_mode

  type ('connection_state, 'query, 'init, 'update) streaming_impl =
    ('connection_state, 'query, 'init, 'update) F.streaming_impl =
    | Pipe of
        ('connection_state
         -> 'query
         -> ('init * 'update Pipe.Reader.t, 'init) Result.t Or_not_authorized.t Deferred.t)
    | Direct of
        ('connection_state
         -> 'query
         -> 'update Implementation_types.Direct_stream_writer.t
         -> ('init, 'init) Result.t Or_not_authorized.t Deferred.t)

  type ('connection_state, 'query, 'init, 'update) streaming_rpc =
    ('connection_state, 'query, 'init, 'update) F.streaming_rpc =
    { bin_query_reader : 'query Bin_prot.Type_class.reader
    ; bin_init_writer : 'init Bin_prot.Type_class.writer
    ; bin_update_writer : 'update Bin_prot.Type_class.writer
    (* 'init can be an error or an initial state *)
    ; impl : ('connection_state, 'query, 'init, 'update) streaming_impl
    }

  type 'connection_state t = 'connection_state F.t =
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

  let sexp_of_t _ = function
    | One_way_expert _ | One_way _ -> [%message "one-way"]
    | Rpc_expert _ | Rpc _ -> [%message "rpc"]
    | Streaming_rpc _ -> [%message "streaming-rpc"]
  ;;

  let lift t ~f =
    match t with
    | One_way (bin_msg, impl) ->
      let impl state str =
        Or_not_authorized.bind_deferred (f state) ~f:(fun authorized_state ->
          impl authorized_state str)
      in
      One_way (bin_msg, impl)
    | One_way_expert impl ->
      One_way_expert
        (fun state buf ~pos ~len ->
           Or_not_authorized.bind_deferred (f state) ~f:(fun authorized_state ->
             impl authorized_state buf ~pos ~len))
    | Rpc (bin_query, bin_response, impl, Blocking) ->
      let impl state q =
        Or_not_authorized.bind (f state) ~f:(fun authorized_state ->
          impl authorized_state q)
      in
      Rpc (bin_query, bin_response, impl, Blocking)
    | Rpc (bin_query, bin_response, impl, Deferred) ->
      let impl state q =
        Or_not_authorized.bind_deferred (f state) ~f:(fun authorized_state ->
          impl authorized_state q)
      in
      Rpc (bin_query, bin_response, impl, Deferred)
    | Rpc_expert (impl, Blocking) ->
      let impl state resp buf ~pos ~len =
        Or_not_authorized.bind (f state) ~f:(fun authorized_state ->
          impl authorized_state resp buf ~pos ~len)
      in
      Rpc_expert (impl, Blocking)
    | Rpc_expert (impl, Deferred) ->
      let impl state resp buf ~pos ~len =
        Or_not_authorized.bind_deferred (f state) ~f:(fun authorized_state ->
          impl authorized_state resp buf ~pos ~len)
      in
      Rpc_expert (impl, Deferred)
    | Streaming_rpc { bin_query_reader; bin_init_writer; bin_update_writer; impl } ->
      let impl =
        match impl with
        | Pipe impl ->
          Pipe
            (fun state q ->
               Or_not_authorized.bind_deferred (f state) ~f:(fun authorized_state ->
                 impl authorized_state q))
        | Direct impl ->
          Direct
            (fun state q w ->
               Or_not_authorized.bind_deferred (f state) ~f:(fun authorized_state ->
                 impl authorized_state q w))
      in
      Streaming_rpc { bin_query_reader; bin_init_writer; bin_update_writer; impl }
  ;;

  let lift_deferred (type a b) (t : b t) ~(f : a -> b Deferred.t) : a t =
    match t with
    | One_way (bin_msg, impl) ->
      One_way
        ( bin_msg
        , fun state str -> Eager_deferred.bind (f state) ~f:(fun state -> impl state str)
        )
    | One_way_expert impl ->
      One_way_expert
        (fun state buf ~pos ~len ->
           Eager_deferred.bind (f state) ~f:(fun state -> impl state buf ~pos ~len))
    | Rpc (bin_query, bin_response, impl, Blocking) ->
      Rpc
        ( bin_query
        , bin_response
        , (fun state q ->
             let%map.Eager_deferred state = f state in
             impl state q)
        , Deferred )
    | Rpc (bin_query, bin_response, impl, Deferred) ->
      Rpc
        ( bin_query
        , bin_response
        , (fun state q ->
             let%bind.Eager_deferred state = f state in
             impl state q)
        , Deferred )
    | Rpc_expert (impl, Deferred) ->
      Rpc_expert
        ( (fun state resp buf ~pos ~len ->
            let%bind.Eager_deferred state = f state in
            impl state resp buf ~pos ~len)
        , Deferred )
    | Rpc_expert (impl, Blocking) ->
      Rpc_expert
        ( (fun state resp buf ~pos ~len ->
            let%map.Eager_deferred state = f state in
            impl state resp buf ~pos ~len)
        , Deferred )
    | Streaming_rpc { bin_query_reader; bin_init_writer; bin_update_writer; impl } ->
      let impl =
        match impl with
        | Pipe impl ->
          Pipe
            (fun state q ->
               let%bind.Eager_deferred state = f state in
               impl state q)
        | Direct impl ->
          Direct
            (fun state q w ->
               let%bind.Eager_deferred state = f state in
               impl state q w)
      in
      Streaming_rpc { bin_query_reader; bin_init_writer; bin_update_writer; impl }
  ;;
end

type 'connection_state t = 'connection_state Implementation_types.Implementation.t =
  { tag : Rpc_tag.t
  ; version : int
  ; f : 'connection_state F.t
  ; shapes : Rpc_shapes.t Lazy.t
  ; on_exception : On_exception.t
  }
[@@deriving sexp_of]

let description t = { Description.name = Rpc_tag.to_string t.tag; version = t.version }
let shapes t = force t.shapes

let lift (type a b) (t : a t) ~(f : b -> a) : b t =
  { t with f = F.lift t.f ~f:(fun b -> Or_not_authorized.Authorized (f b)) }
;;

let with_authorization t ~f = { t with f = F.lift t.f ~f }
let lift_deferred t ~f = { t with f = F.lift_deferred t.f ~f }
let update_on_exception t ~f = { t with on_exception = f t.on_exception }
