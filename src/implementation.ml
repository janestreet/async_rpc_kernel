open Core
open Async_kernel
open Protocol

module Expert = struct
  module Responder = struct
    type t =
      { query_id : Query_id.t
      ; impl_menu_index : Protocol.Impl_menu_index.t
      ; writer : Protocol_writer.t
      ; mutable responded : bool
      }
    [@@deriving sexp_of]

    let create query_id impl_menu_index writer =
      { query_id; impl_menu_index; writer; responded = false }
    ;;
  end

  type implementation_result =
    | Replied
    | Delayed_response of unit Deferred.t
end

module F = struct
  type ('connection_state, 'query, 'init, 'update) streaming_impl =
    | Pipe of
        ('connection_state
         -> 'query
         -> ('init * 'update Pipe.Reader.t, 'init) Result.t Or_not_authorized.t Or_error.t
              Deferred.t)
    | Direct of
        ('connection_state
         -> 'query
         -> 'update Implementation_types.Direct_stream_writer.t
         -> ('init, 'init) Result.t Or_not_authorized.t Or_error.t Deferred.t)

  type ('connection_state, 'query, 'init, 'update) streaming_rpc =
    { bin_query_reader : 'query Bin_prot.Type_class.reader
    ; bin_init_writer : 'init Bin_prot.Type_class.writer
    ; bin_update_writer : 'update Bin_prot.Type_class.writer
        (* 'init can be an error or an initial state *)
    ; impl : ('connection_state, 'query, 'init, 'update) streaming_impl
    ; error_mode : 'init Implementation_mode.Error_mode.t
    ; leave_open_on_exception : bool
    ; here : Source_code_position.t
    }

  type 'connection_state t =
    | One_way :
        'msg Bin_prot.Type_class.reader
        * ('connection_state -> 'msg -> unit Or_not_authorized.t Or_error.t Deferred.t)
        * Source_code_position.t
        -> 'connection_state t
    | One_way_expert :
        ('connection_state
         -> Bigstring.t
         -> pos:int
         -> len:int
         -> unit Or_not_authorized.t Or_error.t Deferred.t)
        -> 'connection_state t
    | Rpc :
        'query Bin_prot.Type_class.reader
        * 'response Bin_prot.Type_class.writer
        * ('connection_state -> 'query -> 'result)
        * 'response Implementation_mode.Error_mode.t
        * ('response, 'result) Implementation_mode.Result_mode.t
        * Source_code_position.t
        -> 'connection_state t
    | Rpc_expert :
        ('connection_state
         -> Expert.Responder.t
         -> Bigstring.t
         -> pos:int
         -> len:int
         -> 'result)
        * (Expert.implementation_result, 'result) Implementation_mode.Result_mode.t
        -> 'connection_state t
    | Streaming_rpc :
        ('connection_state, 'query, 'init, 'update) streaming_rpc
        -> 'connection_state t
    | Menu_rpc : Menu.Stable.V3.response Lazy.t -> 'connection_state t

  let sexp_of_t _ = function
    | One_way_expert _ | One_way _ -> [%message "one-way"]
    | Rpc_expert _ | Rpc _ | Menu_rpc _ -> [%message "rpc"]
    | Streaming_rpc _ -> [%message "streaming-rpc"]
  ;;

  let lift_result_bind_deferred ~f = function
    | Ok (Or_not_authorized.Authorized a) -> f a
    | (Ok (Not_authorized _) | Error _) as t -> Eager_deferred.return t
  ;;

  let lift_result_bind ~f = function
    | Ok (Or_not_authorized.Authorized a) -> f a
    | (Ok (Not_authorized _) | Error _) as t -> t
  ;;

  let lift t ~f =
    match t with
    | One_way (bin_msg, impl, here) ->
      let impl state str =
        lift_result_bind_deferred (f state) ~f:(fun authorized_state ->
          impl authorized_state str)
      in
      One_way (bin_msg, impl, here)
    | One_way_expert impl ->
      One_way_expert
        (fun state buf ~pos ~len ->
          lift_result_bind_deferred (f state) ~f:(fun authorized_state ->
            impl authorized_state buf ~pos ~len))
    | Rpc (bin_query, bin_response, impl, error, Blocking, here) ->
      let impl state q =
        lift_result_bind (f state) ~f:(fun authorized_state -> impl authorized_state q)
      in
      Rpc (bin_query, bin_response, impl, error, Blocking, here)
    | Rpc (bin_query, bin_response, impl, error, Deferred, here) ->
      let impl state q =
        lift_result_bind_deferred (f state) ~f:(fun authorized_state ->
          impl authorized_state q)
      in
      Rpc (bin_query, bin_response, impl, error, Deferred, here)
    | Menu_rpc impl -> Menu_rpc impl
    | Rpc_expert (impl, Blocking) ->
      let impl state resp buf ~pos ~len =
        lift_result_bind (f state) ~f:(fun authorized_state ->
          impl authorized_state resp buf ~pos ~len)
      in
      Rpc_expert (impl, Blocking)
    | Rpc_expert (impl, Deferred) ->
      let impl state resp buf ~pos ~len =
        lift_result_bind_deferred (f state) ~f:(fun authorized_state ->
          impl authorized_state resp buf ~pos ~len)
      in
      Rpc_expert (impl, Deferred)
    | Streaming_rpc
        { bin_query_reader
        ; bin_init_writer
        ; bin_update_writer
        ; impl
        ; error_mode
        ; here
        ; leave_open_on_exception
        } ->
      let impl =
        match impl with
        | Pipe impl ->
          Pipe
            (fun state q ->
              lift_result_bind_deferred (f state) ~f:(fun authorized_state ->
                impl authorized_state q))
        | Direct impl ->
          Direct
            (fun state q w ->
              lift_result_bind_deferred (f state) ~f:(fun authorized_state ->
                impl authorized_state q w))
      in
      Streaming_rpc
        { bin_query_reader
        ; bin_init_writer
        ; bin_update_writer
        ; impl
        ; error_mode
        ; here
        ; leave_open_on_exception
        }
  ;;

  let lift_deferred
    (type a b)
    (t : b t)
    ~f:(lift : a -> b Or_not_authorized.t Or_error.t Deferred.t)
    : a t
    =
    let lift_and_bind x ~f =
      Eager_deferred.bind (lift x) ~f:(fun lift_result ->
        lift_result_bind_deferred lift_result ~f)
    in
    match t with
    | One_way (bin_msg, impl, here) ->
      One_way
        ( bin_msg
        , (fun state str -> lift_and_bind state ~f:(fun state -> impl state str))
        , here )
    | One_way_expert impl ->
      One_way_expert
        (fun state buf ~pos ~len ->
          lift_and_bind state ~f:(fun state -> impl state buf ~pos ~len))
    | Rpc (bin_query, bin_response, impl, error, Blocking, here) ->
      Rpc
        ( bin_query
        , bin_response
        , (fun state q -> lift_and_bind state ~f:(fun state -> return (impl state q)))
        , error
        , Deferred
        , here )
    | Rpc (bin_query, bin_response, impl, error, Deferred, here) ->
      Rpc
        ( bin_query
        , bin_response
        , (fun state q -> lift_and_bind state ~f:(fun state -> impl state q))
        , error
        , Deferred
        , here )
    | Menu_rpc impl -> Menu_rpc impl
    | Rpc_expert (impl, Deferred) ->
      Rpc_expert
        ( (fun state resp buf ~pos ~len ->
            lift_and_bind state ~f:(fun state -> impl state resp buf ~pos ~len))
        , Deferred )
    | Rpc_expert (impl, Blocking) ->
      Rpc_expert
        ( (fun state resp buf ~pos ~len ->
            lift_and_bind state ~f:(fun state -> return (impl state resp buf ~pos ~len)))
        , Deferred )
    | Streaming_rpc
        { bin_query_reader
        ; bin_init_writer
        ; bin_update_writer
        ; impl
        ; error_mode
        ; here
        ; leave_open_on_exception
        } ->
      let impl =
        match impl with
        | Pipe impl ->
          Pipe (fun state q -> lift_and_bind state ~f:(fun state -> impl state q))
        | Direct impl ->
          Direct (fun state q w -> lift_and_bind state ~f:(fun state -> impl state q w))
      in
      Streaming_rpc
        { bin_query_reader
        ; bin_init_writer
        ; bin_update_writer
        ; impl
        ; error_mode
        ; here
        ; leave_open_on_exception
        }
  ;;
end

module Shapes_and_digests = struct
  (* The sexp of Rpc_shapes is the same as the sexp of the digests, so only include them
     once *)
  type t = Rpc_shapes.t * Rpc_shapes.Just_digests.t

  let sexp_of_t ((_, digests) : t) = [%sexp_of: Rpc_shapes.Just_digests.t] digests
end

type 'connection_state t =
  { tag : Rpc_tag.t
  ; version : int
  ; f : 'connection_state F.t
  ; shapes : Shapes_and_digests.t Lazy.t
  ; on_exception : On_exception.t option
  }
[@@deriving sexp_of]

let description t = { Description.name = Rpc_tag.to_string t.tag; version = t.version }
let shapes t = fst (force t.shapes)
let digests t = snd (force t.shapes)

let lift (type a b) (t : a t) ~(f : b -> a) : b t =
  { t with f = F.lift t.f ~f:(fun b -> Ok (Or_not_authorized.Authorized (f b))) }
;;

let try_lift (type a b) (t : a t) ~(f : b -> a Or_error.t) : b t =
  { t with
    f =
      F.lift t.f ~f:(fun b ->
        Or_error.map (f b) ~f:(fun b -> Or_not_authorized.Authorized b))
  }
;;

let with_authorization t ~(f : 'b -> 'a Or_not_authorized.t) =
  { t with f = F.lift t.f ~f:(Fn.compose Or_error.return f) }
;;

let lift_deferred t ~f =
  { t with
    f =
      F.lift_deferred t.f ~f:(fun b ->
        Eager_deferred.map (f b) ~f:(fun x -> Ok (Or_not_authorized.Authorized x)))
  }
;;

let try_lift_deferred t ~f =
  { t with
    f =
      F.lift_deferred t.f ~f:(fun b ->
        Eager_deferred.map
          (f b)
          ~f:(Or_error.map ~f:(fun b -> Or_not_authorized.Authorized b)))
  }
;;

let with_authorization_deferred t ~f =
  { t with
    f = F.lift_deferred t.f ~f:(fun s -> Eager_deferred.map (f s) ~f:Or_error.return)
  }
;;

let update_on_exception t ~f = { t with on_exception = f t.on_exception |> Some }
