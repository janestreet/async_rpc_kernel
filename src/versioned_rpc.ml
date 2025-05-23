open Core
open Async_kernel
open Rpc

let pipe_map_batched t ~f =
  Pipe.map ~max_batch_size:Transport.Writer.transfer_default_max_num_values_per_read t ~f
;;

module Versioned_direct_stream_writer = struct
  module Direct_stream_writer = Pipe_rpc.Direct_stream_writer

  type 'input t =
    | T :
        { convert : 'input -> 'output
        ; writer : 'output Direct_stream_writer.t
        }
        -> 'input t

  let create ~convert ~writer = T { convert; writer }

  let write (T { convert; writer }) input =
    Direct_stream_writer.write writer (convert input)
  ;;

  let write_without_pushback (T { convert; writer }) input =
    Direct_stream_writer.write_without_pushback writer (convert input)
  ;;

  let close (T { convert = _; writer }) = Direct_stream_writer.close writer
  let is_closed (T { convert = _; writer }) = Direct_stream_writer.is_closed writer
  let closed (T { convert = _; writer }) = Direct_stream_writer.closed writer
  let flushed (T { convert = _; writer }) = Direct_stream_writer.flushed writer
end

let failed_conversion x =
  Error.create
    "type conversion failure"
    x
    [%sexp_of:
      [ `Msg | `Query | `Response | `Error | `State | `Update ]
      * [ `Rpc of string ]
      * [ `Version of int ]
      * exn]
;;

let multiple_registrations x =
  Error.create
    "multiple rpc registrations"
    x
    [%sexp_of: [ `Rpc of string ] * [ `Version of int ]]
;;

let unknown_version x = Error.create "unknown rpc version" x [%sexp_of: string * int]

module Callee_converts = struct
  module Rpc = struct
    module Simple = struct
      type ('query, 'response) adapter =
        { adapt :
            'state. ('state -> 'query -> 'response Deferred.t) -> 'state Implementation.t
        }

      type ('query, 'response) t =
        { name : string
        ; adapters : ('query, 'response) adapter Int.Map.t
        }
      [@@deriving fields ~getters]

      let create ~name = { name; adapters = Int.Map.empty }

      let wrap_error fn state query =
        match%map fn state query with
        | Ok value -> Ok value
        | Error error -> Error (Error.to_string_hum error)
      ;;

      let add { name; adapters } rpc adapter =
        if String.( <> ) name (Rpc.name rpc)
        then
          Or_error.error
            "Rpc names don't agree"
            (name, Rpc.name rpc)
            [%sexp_of: string * string]
        else (
          let version = Rpc.version rpc in
          match Map.find adapters version with
          | Some _ ->
            Or_error.error
              "Version already exists"
              (name, version)
              [%sexp_of: string * int]
          | None ->
            let adapters = Map.set adapters ~key:version ~data:adapter in
            Ok { name; adapters })
      ;;

      let add_rpc_version t old_rpc upgrade downgrade =
        let adapt fn =
          let adapted state old_query =
            let%map result = fn state (upgrade old_query) in
            downgrade result
          in
          Rpc.implement old_rpc adapted
        in
        add t old_rpc { adapt }
      ;;

      let add_rpc_version_with_failure t old_rpc upgrade_or_error downgrade_or_error =
        let adapt fn =
          let adapted state old_query =
            let open Deferred.Result.Let_syntax in
            let%bind query = Deferred.return (upgrade_or_error old_query) in
            let%bind response = fn state query in
            Deferred.return (downgrade_or_error response)
          in
          Rpc.implement old_rpc (wrap_error adapted)
        in
        add t old_rpc { adapt }
      ;;

      let add_version t ~version ~bin_query ~bin_response upgrade downgrade =
        let rpc =
          Rpc.create
            ~name:t.name
            ~version
            ~bin_query
            ~bin_response
            ~include_in_error_count:Only_on_exn
        in
        add_rpc_version t rpc upgrade downgrade
      ;;

      let add_version_with_failure t ~version ~bin_query ~bin_response upgrade downgrade =
        let rpc =
          Rpc.create
            ~name:t.name
            ~version
            ~bin_query
            ~bin_response
            ~include_in_error_count:Result
        in
        add_rpc_version_with_failure t rpc upgrade downgrade
      ;;

      let implement t fn = Map.data t.adapters |> List.map ~f:(fun { adapt } -> adapt fn)
    end

    module type S = sig
      type query
      type response

      val implement_multi
        :  ?log_not_previously_seen_version:(name:string -> int -> unit)
        -> ('state -> version:int -> query -> response Deferred.t)
        -> 'state Implementation.t list

      val rpcs : unit -> Any.t list
      val versions : unit -> Int.Set.t
      val name : string
    end

    module Make (Model : sig
        val name : string

        type query
        type response
      end) =
    struct
      let name = Model.name

      type 's impl = 's -> version:int -> Model.query -> Model.response Deferred.t

      type implementer =
        { implement : 's. log_version:(int -> unit) -> 's impl -> 's Implementation.t }

      let registry : (int, implementer * Any.t) Hashtbl.t = Int.Table.create ~size:1 ()

      let implement_multi ?log_not_previously_seen_version f =
        let log_version =
          match log_not_previously_seen_version with
          | None -> ignore
          (* prevent calling [f] more than once per version *)
          | Some f -> Memo.general (f ~name)
        in
        List.map (Hashtbl.data registry) ~f:(fun (i, _rpc) -> i.implement ~log_version f)
      ;;

      let rpcs () = List.map (Hashtbl.data registry) ~f:(fun (_, rpc) -> rpc)
      let versions () = Int.Set.of_list (Hashtbl.keys registry)

      module Register (Version_i : sig
          type query [@@deriving bin_io]
          type response [@@deriving bin_io]

          val version : int
          val model_of_query : query -> Model.query
          val response_of_model : Model.response -> response
        end) =
      struct
        open Version_i

        let rpc =
          Rpc.create
            ~name
            ~version
            ~bin_query
            ~bin_response
            ~include_in_error_count:Only_on_exn
        ;;

        let () =
          let implement ~log_version f =
            Rpc.implement rpc (fun s q ->
              log_version version;
              match Result.try_with (fun () -> Version_i.model_of_query q) with
              | Error exn ->
                Error.raise (failed_conversion (`Query, `Rpc name, `Version version, exn))
              | Ok q ->
                let%map r = f s ~version q in
                (match Result.try_with (fun () -> Version_i.response_of_model r) with
                 | Ok r -> r
                 | Error exn ->
                   Error.raise
                     (failed_conversion (`Response, `Rpc name, `Version version, exn))))
          in
          match Hashtbl.find registry version with
          | None -> Hashtbl.set registry ~key:version ~data:({ implement }, Any.Rpc rpc)
          | Some _ -> Error.raise (multiple_registrations (`Rpc name, `Version version))
        ;;
      end
    end
  end

  module Pipe_rpc = struct
    module type S = sig
      type query
      type response
      type error

      val implement_multi
        :  ?log_not_previously_seen_version:(name:string -> int -> unit)
        -> ('state
            -> version:int
            -> query
            -> (response Pipe.Reader.t, error) Result.t Deferred.t)
        -> 'state Implementation.t list

      val implement_direct_multi
        :  ?log_not_previously_seen_version:(name:string -> int -> unit)
        -> ('state
            -> version:int
            -> query
            -> response Versioned_direct_stream_writer.t
            -> (unit, error) Result.t Deferred.t)
        -> 'state Implementation.t list

      val rpcs : unit -> Any.t list
      val versions : unit -> Int.Set.t
      val name : string
    end

    module Make (Model : sig
        val name : string

        type query
        type response
        type error
      end) =
    struct
      let name = Model.name

      type 's impl =
        | Pipe of
            ('s
             -> version:int
             -> Model.query
             -> (Model.response Pipe.Reader.t, Model.error) Result.t Deferred.t)
        | Direct of
            ('s
             -> version:int
             -> Model.query
             -> Model.response Versioned_direct_stream_writer.t
             -> (unit, Model.error) Result.t Deferred.t)

      type implementer =
        { implement : 's. log_version:(int -> unit) -> 's impl -> 's Implementation.t }

      let registry = Int.Table.create ~size:1 ()

      let implement_multi_gen ?log_not_previously_seen_version impl =
        let log_version =
          match log_not_previously_seen_version with
          | None -> ignore
          (* prevent calling [f] more than once per version *)
          | Some f -> Memo.general (f ~name)
        in
        List.map (Hashtbl.data registry) ~f:(fun (i, _) -> i.implement ~log_version impl)
      ;;

      let implement_multi ?log_not_previously_seen_version f =
        implement_multi_gen ?log_not_previously_seen_version (Pipe f)
      ;;

      let implement_direct_multi ?log_not_previously_seen_version f =
        implement_multi_gen ?log_not_previously_seen_version (Direct f)
      ;;

      let rpcs () = List.map (Hashtbl.data registry) ~f:(fun (_, rpc) -> rpc)
      let versions () = Int.Set.of_list (Hashtbl.keys registry)

      module type Version_shared = sig
        type query [@@deriving bin_io]
        type response [@@deriving bin_io]
        type error [@@deriving bin_io]

        val version : int
        val model_of_query : query -> Model.query
        val error_of_model : Model.error -> error
        val client_pushes_back : bool
      end

      module Make_shared
          (Version_i : Version_shared)
          (Convert : sig
             val convert_elt : (Model.response -> Version_i.response) Or_error.t

             val convert_pipe
               :  Model.response Pipe.Reader.t
               -> Version_i.response Pipe.Reader.t
           end) =
      struct
        open Version_i
        open Convert

        let rpc =
          Pipe_rpc.create
            ~name
            ~version
            ~bin_query
            ~bin_response
            ~bin_error
            ?client_pushes_back:(Option.some_if client_pushes_back ())
            ()
        ;;

        let wrapped_model_of_query q =
          match Version_i.model_of_query q with
          | exception exn ->
            Error.raise (failed_conversion (`Response, `Rpc name, `Version version, exn))
          | q -> q
        ;;

        let wrapped_error_of_model error =
          match Version_i.error_of_model error with
          | error -> Error error
          | exception exn ->
            Error.raise (failed_conversion (`Error, `Rpc name, `Version version, exn))
        ;;

        let implement ~log_version impl =
          match impl with
          | Pipe f ->
            Pipe_rpc.implement
              rpc
              (fun s q ->
                log_version version;
                match%bind f s ~version (wrapped_model_of_query q) with
                | Ok pipe ->
                  Monitor.handle_errors
                    (fun () -> return (Ok (convert_pipe pipe)))
                    (fun exn ->
                      Error.raise
                        (failed_conversion (`Response, `Rpc name, `Version version, exn)))
                | Error error -> return (wrapped_error_of_model error))
              ~leave_open_on_exception:true
          | Direct f ->
            let convert_elt = Or_error.ok_exn convert_elt in
            Pipe_rpc.implement_direct
              rpc
              (fun s q dsw ->
                let writer =
                  Versioned_direct_stream_writer.create ~convert:convert_elt ~writer:dsw
                in
                match%map f s ~version (wrapped_model_of_query q) writer with
                | Ok () -> Ok ()
                | Error error -> wrapped_error_of_model error)
              ~leave_open_on_exception:true
        ;;

        let () =
          match Hashtbl.find registry version with
          | None -> Hashtbl.set registry ~key:version ~data:({ implement }, Any.Pipe rpc)
          | Some _ -> Error.raise (multiple_registrations (`Rpc name, `Version version))
        ;;
      end

      module Register_raw (Version_i : sig
          include Version_shared

          val response_of_model : Model.response Pipe.Reader.t -> response Pipe.Reader.t
        end) =
        Make_shared
          (Version_i)
          (struct
            let convert_elt =
              Or_error.error_string "cannot use direct interface with Register_raw"
            ;;

            let convert_pipe = Version_i.response_of_model
          end)

      module Register (Version_i : sig
          include Version_shared

          val response_of_model : Model.response -> response
        end) =
        Make_shared
          (Version_i)
          (struct
            let convert_elt = Ok Version_i.response_of_model
            let convert_pipe pipe = pipe_map_batched pipe ~f:Version_i.response_of_model
          end)
    end
  end

  module State_rpc = struct
    module type S = sig
      type query
      type state
      type update
      type error

      val implement_multi
        :  ?log_not_previously_seen_version:(name:string -> int -> unit)
        -> ('connection_state
            -> version:int
            -> query
            -> (state * update Pipe.Reader.t, error) Result.t Deferred.t)
        -> 'connection_state Implementation.t list

      val rpcs : unit -> Any.t list
      val versions : unit -> Int.Set.t
      val name : string
    end

    module Make (Model : sig
        val name : string

        type query
        type state
        type update
        type error
      end) =
    struct
      let name = Model.name

      type 's impl =
        's
        -> version:int
        -> Model.query
        -> (Model.state * Model.update Pipe.Reader.t, Model.error) Result.t Deferred.t

      type implementer =
        { implement : 's. log_version:(int -> unit) -> 's impl -> 's Implementation.t }

      let registry = Int.Table.create ~size:1 ()

      let implement_multi ?log_not_previously_seen_version f =
        let log_version =
          match log_not_previously_seen_version with
          | None -> ignore
          (* prevent calling [f] more than once per version *)
          | Some f -> Memo.general (f ~name)
        in
        List.map (Hashtbl.data registry) ~f:(fun (i, _) -> i.implement ~log_version f)
      ;;

      let rpcs () = List.map (Hashtbl.data registry) ~f:(fun (_, rpc) -> rpc)
      let versions () = Int.Set.of_list (Hashtbl.keys registry)

      module type Version_shared = sig
        type query [@@deriving bin_io]
        type state [@@deriving bin_io]
        type update [@@deriving bin_io]
        type error [@@deriving bin_io]

        val version : int
        val model_of_query : query -> Model.query
        val state_of_model : Model.state -> state
        val error_of_model : Model.error -> error
        val client_pushes_back : bool
      end

      module Register_raw (Version_i : sig
          include Version_shared

          val update_of_model
            :  Model.state
            -> Model.update Pipe.Reader.t
            -> update Pipe.Reader.t
        end) =
      struct
        open Version_i

        let rpc =
          State_rpc.create
            ~name
            ~version
            ~bin_query
            ~bin_state
            ~bin_update
            ~bin_error
            ?client_pushes_back:(Option.some_if client_pushes_back ())
            ()
        ;;

        let () =
          let implement ~log_version f =
            State_rpc.implement
              rpc
              (fun s q ->
                log_version version;
                match Version_i.model_of_query q with
                | exception exn ->
                  Error.raise
                    (failed_conversion (`Response, `Rpc name, `Version version, exn))
                | q ->
                  (match%bind f s ~version q with
                   | Ok (model_state, pipe) ->
                     let state =
                       match Version_i.state_of_model model_state with
                       | state -> state
                       | exception exn ->
                         Error.raise
                           (failed_conversion (`State, `Rpc name, `Version version, exn))
                     in
                     Monitor.handle_errors
                       (fun () ->
                         return (Ok (state, Version_i.update_of_model model_state pipe)))
                       (fun exn ->
                         Error.raise
                           (failed_conversion (`Update, `Rpc name, `Version version, exn)))
                   | Error error ->
                     return
                       (match Version_i.error_of_model error with
                        | error -> Error error
                        | exception exn ->
                          Error.raise
                            (failed_conversion (`Error, `Rpc name, `Version version, exn)))))
              ~leave_open_on_exception:true
          in
          match Hashtbl.find registry version with
          | None -> Hashtbl.set registry ~key:version ~data:({ implement }, Any.State rpc)
          | Some _ -> Error.raise (multiple_registrations (`Rpc name, `Version version))
        ;;
      end

      module Register (Version_i : sig
          include Version_shared

          val update_of_model : Model.update -> update
        end) =
      struct
        include Register_raw (struct
            include Version_i

            let update_of_model _state pipe = pipe_map_batched ~f:update_of_model pipe
          end)
      end
    end
  end

  module One_way = struct
    module type S = sig
      type msg

      val implement_multi
        :  ?log_not_previously_seen_version:(name:string -> int -> unit)
        -> ('state -> version:int -> msg -> unit)
        -> 'state Implementation.t list

      val rpcs : unit -> Any.t list
      val versions : unit -> Int.Set.t
      val name : string
    end

    module Make (Model : sig
        val name : string

        type msg
      end) =
    struct
      let name = Model.name

      type 's impl = 's -> version:int -> Model.msg -> unit

      type implementer =
        { implement : 's. log_version:(int -> unit) -> 's impl -> 's Implementation.t }

      let registry : (int, implementer * Any.t) Hashtbl.t = Int.Table.create ~size:1 ()

      let implement_multi ?log_not_previously_seen_version f =
        let log_version =
          match log_not_previously_seen_version with
          | None -> ignore
          (* prevent calling [f] more than once per version *)
          | Some f -> Memo.general (f ~name)
        in
        List.map (Hashtbl.data registry) ~f:(fun (i, _rpc) -> i.implement ~log_version f)
      ;;

      let rpcs () = List.map (Hashtbl.data registry) ~f:(fun (_, rpc) -> rpc)
      let versions () = Int.Set.of_list (Hashtbl.keys registry)

      module Register (Version_i : sig
          type msg [@@deriving bin_io]

          val version : int
          val model_of_msg : msg -> Model.msg
        end) =
      struct
        open Version_i

        let rpc = One_way.create ~name ~version ~bin_msg

        let () =
          let implement ~log_version f =
            One_way.implement
              rpc
              (fun s q ->
                log_version version;
                match Result.try_with (fun () -> Version_i.model_of_msg q) with
                | Error exn ->
                  Error.raise (failed_conversion (`Msg, `Rpc name, `Version version, exn))
                | Ok q -> f s ~version q)
              ~on_exception:Close_connection
          in
          match Hashtbl.find registry version with
          | None ->
            Hashtbl.set registry ~key:version ~data:({ implement }, Any.One_way rpc)
          | Some _ -> Error.raise (multiple_registrations (`Rpc name, `Version version))
        ;;
      end
    end
  end
end

module Menu = struct
  include Menu

  let create descriptions = of_supported_rpcs descriptions ~rpc_shapes:`Unknown

  let rpc =
    Rpc.create
      ~name:Menu.version_menu_rpc_name
      ~version:1
      ~bin_query:[%bin_type_class: Stable.V1.query]
      ~bin_response:[%bin_type_class: Stable.V1.response]
      ~include_in_error_count:Only_on_exn
  ;;

  let supported_versions = Menu.supported_versions

  let aux_request dispatch conn =
    let%map result = dispatch rpc conn () in
    Result.map result ~f:of_v1_response
  ;;

  let request conn =
    match%bind.Eager_deferred.Or_error Connection.peer_menu conn with
    | Some menu -> return (Ok menu)
    | None -> aux_request Rpc.dispatch conn
  ;;

  let request' conn =
    match%bind.Eager_deferred.Result Connection.peer_menu' conn with
    | Some menu -> return (Ok menu)
    | None -> aux_request Rpc.dispatch' conn
  ;;

  let implement f = Rpc.implement rpc f

  module With_shapes = struct
    module Model = struct
      let name = "__Versioned_rpc.Shape_menu"

      type query = unit
      type response = (Description.t * Rpc_shapes.t) list
    end

    include Callee_converts.Rpc.Make (Model)

    module V1 = struct
      module T = struct
        let version = 1

        type query = unit [@@deriving bin_io]

        let%expect_test _ =
          print_endline [%bin_digest: query];
          [%expect {| 86ba5df747eec837f0b391dd49f33f9e |}]
        ;;

        type response = (Description.Stable.V1.t * Rpc_shapes.Stable.V1.t) list
        [@@deriving bin_io]

        let%expect_test _ =
          print_endline [%bin_digest: response];
          [%expect {| b4bdc01b7ead907e6d0d3f3d89700f1b |}]
        ;;

        let model_of_query = Fn.id
        let response_of_model = Fn.id
      end

      include T
      include Register (T)
    end

    module Current_version = V1

    type t = (Description.t * Rpc_shapes.t) list

    let add impls =
      let shape_menu =
        List.map impls ~f:(fun impl ->
          Implementation.description impl, Implementation.shapes impl)
      in
      let shape_menu_impls = implement_multi (fun _ ~version:_ () -> return shape_menu) in
      impls @ shape_menu_impls
    ;;

    let request conn = Rpc.dispatch Current_version.rpc conn ()
  end
end

module Connection_with_menu = struct
  type t =
    { connection : Connection.t
    ; menu : Menu.t
    }
  [@@deriving fields ~getters]

  let create connection =
    let open Deferred.Or_error.Let_syntax in
    let%map menu = Menu.request connection in
    { connection; menu }
  ;;

  let create_directly connection menu = { connection; menu }
end

module Caller_converts = struct
  module Dispatch = struct
    module Make (M : Monad) = struct
      open M

      let with_specific_version ~version ~connection ~name ~query ~dispatcher ~registry =
        match Hashtbl.find registry version with
        | None -> return (Error (unknown_version (name, version)))
        | Some (dispatch, _rpc) -> dispatcher dispatch connection query
      ;;

      let with_version_menu
        { Connection_with_menu.connection; menu }
        query
        ~name
        ~versions
        ~registry
        ~dispatcher
        =
        let caller_versions = versions () in
        match
          Menu.highest_shared_version ~rpc_name:name ~callee_menu:menu ~caller_versions
        with
        | Error e -> return (Error e)
        | Ok version ->
          with_specific_version ~version ~connection ~name ~query ~registry ~dispatcher
      ;;
    end

    module Async = Make (Deferred)
    module Direct = Make (Monad.Ident)
  end

  module Rpc = struct
    module type S = sig
      type query
      type response

      val dispatch_multi
        :  Connection_with_menu.t
        -> query
        -> response Or_error.t Deferred.t

      val rpcs : unit -> Any.t list
      val versions : unit -> Int.Set.t
      val name : string
    end

    module Make (Model : sig
        val name : string

        type query
        type response
      end) =
    struct
      let name = Model.name
      let registry = Int.Table.create ~size:1 ()
      let rpcs () = List.map (Hashtbl.data registry) ~f:(fun (_, rpc) -> rpc)
      let versions () = Int.Set.of_list (Hashtbl.keys registry)

      let dispatch_multi conn_with_menu query =
        Dispatch.Async.with_version_menu
          conn_with_menu
          query
          ~name
          ~versions
          ~registry
          ~dispatcher:Fn.id
      ;;

      module Register' (Version_i : sig
          type query [@@deriving bin_io]
          type response [@@deriving bin_io]

          val version : int
          val query_of_model : Model.query -> query
          val model_of_response : Model.query -> response -> Model.response
        end) =
      struct
        open Version_i

        let rpc =
          Rpc.create
            ~name
            ~version
            ~bin_query
            ~bin_response
            ~include_in_error_count:Only_on_exn
        ;;

        let () =
          let dispatch conn mq =
            match Result.try_with (fun () -> Version_i.query_of_model mq) with
            | Error exn ->
              return
                (Error (failed_conversion (`Query, `Rpc name, `Version version, exn)))
            | Ok q ->
              let%map result = Rpc.dispatch rpc conn q in
              Result.bind result ~f:(fun r ->
                match Result.try_with (fun () -> Version_i.model_of_response mq r) with
                | Ok r -> Ok r
                | Error exn ->
                  Error (failed_conversion (`Response, `Rpc name, `Version version, exn)))
          in
          match Hashtbl.find registry version with
          | None -> Hashtbl.set registry ~key:version ~data:(dispatch, Any.Rpc rpc)
          | Some _ -> Error.raise (multiple_registrations (`Rpc name, `Version version))
        ;;
      end

      module Register (Version_i : sig
          type query [@@deriving bin_io]
          type response [@@deriving bin_io]

          val version : int
          val query_of_model : Model.query -> query
          val model_of_response : response -> Model.response
        end) =
      Register' (struct
          include Version_i

          let model_of_response _ r = model_of_response r
        end)
    end
  end

  module Pipe_rpc = struct
    module type S = sig
      type query
      type response
      type error

      val dispatch_multi
        :  Connection_with_menu.t
        -> query
        -> (response Or_error.t Pipe.Reader.t * Pipe_rpc.Metadata.t, error) Result.t
             Or_error.t
             Deferred.t

      val dispatch_iter_multi
        :  Connection_with_menu.t
        -> query
        -> f:(response Pipe_rpc.Pipe_message.t -> Pipe_rpc.Pipe_response.t)
        -> (Pipe_rpc.Id.t, error) Result.t Or_error.t Deferred.t

      val abort_multi : Connection_with_menu.t -> Pipe_rpc.Id.t -> unit Or_error.t
      val rpcs : unit -> Any.t list
      val versions : unit -> Int.Set.t
      val name : string
    end

    module Make (Model : sig
        val name : string

        type query
        type response
        type error
      end) =
    struct
      type dispatcher =
        { abort : Connection.t -> Pipe_rpc.Id.t -> unit
        ; dispatch :
            Connection.t
            -> Model.query
            -> ( Model.response Or_error.t Pipe.Reader.t * Pipe_rpc.Metadata.t
                 , Model.error )
                 Result.t
                 Or_error.t
                 Deferred.t
        ; dispatch_iter :
            Connection.t
            -> Model.query
            -> f:(Model.response Pipe_rpc.Pipe_message.t -> Pipe_rpc.Pipe_response.t)
            -> (Pipe_rpc.Id.t, Model.error) Result.t Or_error.t Deferred.t
        }

      let name = Model.name
      let registry : (dispatcher * Any.t) Int.Table.t = Int.Table.create ~size:1 ()
      let rpcs () = List.map (Hashtbl.data registry) ~f:(fun (_, rpc) -> rpc)
      let versions () = Int.Set.of_list (Hashtbl.keys registry)

      let dispatch_iter_multi conn_with_menu query ~f =
        Dispatch.Async.with_version_menu
          conn_with_menu
          query
          ~name
          ~versions
          ~registry
          ~dispatcher:(fun { dispatch_iter; _ } conn query -> dispatch_iter conn query ~f)
      ;;

      let dispatch_multi conn_with_menu query =
        Dispatch.Async.with_version_menu
          conn_with_menu
          query
          ~name
          ~versions
          ~registry
          ~dispatcher:(fun { dispatch; _ } conn query -> dispatch conn query)
      ;;

      let abort_multi conn_with_menu id =
        Dispatch.Direct.with_version_menu
          conn_with_menu
          id
          ~name
          ~versions
          ~registry
          ~dispatcher:(fun { abort; _ } conn id ->
            abort conn id;
            Ok ())
      ;;

      module type Version_shared = sig
        type query [@@deriving bin_io]
        type response [@@deriving bin_io]
        type error [@@deriving bin_io]

        val version : int
        val query_of_model : Model.query -> query
        val model_of_error : error -> Model.error
        val client_pushes_back : bool
      end

      module Make_shared
          (Version_i : Version_shared)
          (Convert : sig
             val convert_elt : (Version_i.response -> Model.response) Or_error.t

             val convert_pipe
               :  Version_i.response Pipe.Reader.t
               -> Model.response Or_error.t Pipe.Reader.t
           end) =
      struct
        open Version_i
        open Convert

        let rpc =
          Pipe_rpc.create
            ~name
            ~version
            ~bin_query
            ~bin_response
            ~bin_error
            ?client_pushes_back:(Option.some_if client_pushes_back ())
            ()
        ;;

        let wrapped_query_of_model q =
          match Version_i.query_of_model q with
          | exception exn ->
            return (Error (failed_conversion (`Query, `Rpc name, `Version version, exn)))
          | q -> return (Ok q)
        ;;

        let convert_result result ~convert_ok =
          match result with
          | Error _ as e -> e
          | Ok (Error e) ->
            (match Version_i.model_of_error e with
             | e' -> Ok (Error e')
             | exception exn ->
               Error (failed_conversion (`Error, `Rpc name, `Version version, exn)))
          | Ok (Ok ok) -> Ok (Ok (convert_ok ok))
        ;;

        let dispatch conn q =
          wrapped_query_of_model q
          >>=? fun q ->
          let%map result = Pipe_rpc.dispatch rpc conn q in
          convert_result result ~convert_ok:(fun (pipe, id) -> convert_pipe pipe, id)
        ;;

        let dispatch_iter conn q ~f =
          let convert_elt = Or_error.ok_exn convert_elt in
          wrapped_query_of_model q
          >>=? fun q ->
          let convert_message (m : _ Pipe_rpc.Pipe_message.t) =
            match m with
            | Closed _ as closed -> closed
            | Update u -> Update (convert_elt u)
          in
          let%map result =
            Pipe_rpc.dispatch_iter rpc conn q ~f:(fun message ->
              f (convert_message message))
          in
          convert_result result ~convert_ok:Fn.id
        ;;

        let abort conn id = Pipe_rpc.abort rpc conn id

        let () =
          match Hashtbl.find registry version with
          | None ->
            Hashtbl.set
              registry
              ~key:version
              ~data:({ abort; dispatch; dispatch_iter }, Any.Pipe rpc)
          | Some _ -> Error.raise (multiple_registrations (`Rpc name, `Version version))
        ;;
      end

      module Register_raw (Version_i : sig
          include Version_shared

          val model_of_response
            :  response Pipe.Reader.t
            -> Model.response Or_error.t Pipe.Reader.t
        end) =
        Make_shared
          (Version_i)
          (struct
            let convert_elt = Or_error.error_string "Cannot use Direct with Register_raw"
            let convert_pipe = Version_i.model_of_response
          end)

      module Register (Version_i : sig
          include Version_shared

          val model_of_response : response -> Model.response
        end) =
        Make_shared
          (Version_i)
          (struct
            let convert_elt = Ok Version_i.model_of_response

            let convert_pipe rs =
              pipe_map_batched rs ~f:(fun r ->
                match Version_i.model_of_response r with
                | r -> Ok r
                | exception exn ->
                  Error
                    (failed_conversion
                       (`Response, `Rpc name, `Version Version_i.version, exn)))
            ;;
          end)
    end
  end

  module State_rpc = struct
    module type S = sig
      type query
      type state
      type update
      type error

      val dispatch_multi
        :  Connection_with_menu.t
        -> query
        -> ( state * update Or_error.t Pipe.Reader.t * State_rpc.Metadata.t
             , error )
             Result.t
             Or_error.t
             Deferred.t

      val rpcs : unit -> Any.t list
      val versions : unit -> Int.Set.t
      val name : string
    end

    module Make (Model : sig
        val name : string

        type query
        type state
        type update
        type error
      end) =
    struct
      let name = Model.name
      let registry = Int.Table.create ~size:1 ()
      let rpcs () = List.map (Hashtbl.data registry) ~f:(fun (_, rpc) -> rpc)
      let versions () = Int.Set.of_list (Hashtbl.keys registry)

      let dispatch_multi conn_with_menu query =
        Dispatch.Async.with_version_menu
          conn_with_menu
          query
          ~name
          ~versions
          ~registry
          ~dispatcher:Fn.id
      ;;

      module type Version_shared = sig
        type query [@@deriving bin_io]
        type state [@@deriving bin_io]
        type update [@@deriving bin_io]
        type error [@@deriving bin_io]

        val version : int
        val query_of_model : Model.query -> query
        val model_of_state : state -> Model.state
        val model_of_error : error -> Model.error
        val client_pushes_back : bool
      end

      module Register_raw (Version_i : sig
          include Version_shared

          val model_of_update
            :  update Pipe.Reader.t
            -> Model.update Or_error.t Pipe.Reader.t
        end) =
      struct
        open Version_i

        let rpc =
          State_rpc.create
            ~name
            ~version
            ~bin_query
            ~bin_state
            ~bin_update
            ~bin_error
            ?client_pushes_back:(Option.some_if client_pushes_back ())
            ()
        ;;

        let () =
          let dispatch conn q =
            match Version_i.query_of_model q with
            | exception exn ->
              return
                (Error (failed_conversion (`Query, `Rpc name, `Version version, exn)))
            | q ->
              let%map result = State_rpc.dispatch rpc conn q in
              (match result with
               | Error exn -> Error exn
               | Ok (Error e) ->
                 (match Version_i.model_of_error e with
                  | e' -> Ok (Error e')
                  | exception exn ->
                    Error (failed_conversion (`Error, `Rpc name, `Version version, exn)))
               | Ok (Ok (state, pipe, id)) ->
                 (match Version_i.model_of_state state with
                  | exception exn ->
                    Error (failed_conversion (`State, `Rpc name, `Version version, exn))
                  | state -> Ok (Ok (state, Version_i.model_of_update pipe, id))))
          in
          match Hashtbl.find registry version with
          | None -> Hashtbl.set registry ~key:version ~data:(dispatch, Any.State rpc)
          | Some _ -> Error.raise (multiple_registrations (`Rpc name, `Version version))
        ;;
      end

      module Register (Version_i : sig
          include Version_shared

          val model_of_update : update -> Model.update
        end) =
      struct
        include Register_raw (struct
            include Version_i

            let model_of_update rs =
              pipe_map_batched rs ~f:(fun r ->
                match Version_i.model_of_update r with
                | r -> Ok r
                | exception exn ->
                  Error (failed_conversion (`Update, `Rpc name, `Version version, exn)))
            ;;
          end)
      end
    end
  end

  module One_way = struct
    module type S = sig
      type msg

      val dispatch_multi : Connection_with_menu.t -> msg -> unit Or_error.t
      val rpcs : unit -> Any.t list
      val versions : unit -> Int.Set.t
      val name : string
    end

    module Make (Model : sig
        val name : string

        type msg
      end) =
    struct
      let name = Model.name
      let registry = Int.Table.create ~size:1 ()
      let rpcs () = List.map (Hashtbl.data registry) ~f:(fun (_, rpc) -> rpc)
      let versions () = Int.Set.of_list (Hashtbl.keys registry)

      let dispatch_multi conn_with_menu msg =
        Dispatch.Direct.with_version_menu
          conn_with_menu
          msg
          ~name
          ~versions
          ~registry
          ~dispatcher:Fn.id
      ;;

      module Register (Version_i : sig
          type msg [@@deriving bin_io]

          val version : int
          val msg_of_model : Model.msg -> msg
        end) =
      struct
        open Version_i

        let rpc = One_way.create ~name ~version ~bin_msg

        let () =
          let dispatch conn q =
            match Result.try_with (fun () -> Version_i.msg_of_model q) with
            | Error exn ->
              Error (failed_conversion (`Msg, `Rpc name, `Version version, exn))
            | Ok q -> One_way.dispatch rpc conn q
          in
          match Hashtbl.find registry version with
          | None -> Hashtbl.set registry ~key:version ~data:(dispatch, Any.One_way rpc)
          | Some _ -> Error.raise (multiple_registrations (`Rpc name, `Version version))
        ;;
      end
    end
  end
end

module Both_convert = struct
  module Plain = struct
    module type S = sig
      type caller_query
      type callee_query
      type caller_response
      type callee_response

      val dispatch_multi
        :  Connection_with_menu.t
        -> caller_query
        -> caller_response Or_error.t Deferred.t

      val implement_multi
        :  ?log_not_previously_seen_version:(name:string -> int -> unit)
        -> ('state -> version:int -> callee_query -> callee_response Deferred.t)
        -> 'state Implementation.t list

      val rpcs : unit -> Any.t list
      val versions : unit -> Int.Set.t
      val name : string
    end

    module Make (Model : sig
        val name : string

        module Caller : sig
          type query
          type response
        end

        module Callee : sig
          type query
          type response
        end
      end) =
    struct
      open Model

      let name = name

      module Caller = Caller_converts.Rpc.Make (struct
          let name = name

          include Caller
        end)

      module Callee = Callee_converts.Rpc.Make (struct
          let name = name

          include Callee
        end)

      let%test _ = Int.Set.equal (Caller.versions ()) (Callee.versions ())

      module Register (Version : sig
          open Model

          val version : int

          type query [@@deriving bin_io]
          type response [@@deriving bin_io]

          val query_of_caller_model : Caller.query -> query
          val callee_model_of_query : query -> Callee.query
          val response_of_callee_model : Callee.response -> response
          val caller_model_of_response : response -> Caller.response
        end) =
      struct
        include Callee.Register (struct
            include Version

            let model_of_query = callee_model_of_query
            let response_of_model = response_of_callee_model
          end)

        include Caller.Register (struct
            include Version

            let query_of_model = query_of_caller_model
            let model_of_response = caller_model_of_response
          end)

        let%test _ = Int.Set.equal (Caller.versions ()) (Callee.versions ())
      end

      let dispatch_multi = Caller.dispatch_multi
      let implement_multi = Callee.implement_multi

      (* Note: Caller.versions is the same as Callee.versions, so it doesn't matter which
         one we call here. Same for [rpcs]. *)
      let versions () = Caller.versions ()
      let rpcs () = Caller.rpcs ()
    end
  end

  module Pipe_rpc = struct
    module type S = sig
      type caller_query
      type callee_query
      type caller_response
      type callee_response
      type caller_error
      type callee_error

      val dispatch_multi
        :  Connection_with_menu.t
        -> caller_query
        -> ( caller_response Or_error.t Pipe.Reader.t * Pipe_rpc.Metadata.t
             , caller_error )
             Result.t
             Or_error.t
             Deferred.t

      val dispatch_iter_multi
        :  Connection_with_menu.t
        -> caller_query
        -> f:(caller_response Pipe_rpc.Pipe_message.t -> Pipe_rpc.Pipe_response.t)
        -> (Pipe_rpc.Id.t, caller_error) Result.t Or_error.t Deferred.t

      val abort_multi : Connection_with_menu.t -> Pipe_rpc.Id.t -> unit Or_error.t

      val implement_multi
        :  ?log_not_previously_seen_version:(name:string -> int -> unit)
        -> ('state
            -> version:int
            -> callee_query
            -> (callee_response Pipe.Reader.t, callee_error) Result.t Deferred.t)
        -> 'state Implementation.t list

      val implement_direct_multi
        :  ?log_not_previously_seen_version:(name:string -> int -> unit)
        -> ('state
            -> version:int
            -> callee_query
            -> callee_response Versioned_direct_stream_writer.t
            -> (unit, callee_error) Result.t Deferred.t)
        -> 'state Implementation.t list

      val rpcs : unit -> Any.t list
      val versions : unit -> Int.Set.t
      val name : string
    end

    module Make (Model : sig
        val name : string

        module Caller : sig
          type query
          type response
          type error
        end

        module Callee : sig
          type query
          type response
          type error
        end
      end) =
    struct
      open Model

      let name = name

      module Caller = Caller_converts.Pipe_rpc.Make (struct
          let name = name

          include Caller
        end)

      module Callee = Callee_converts.Pipe_rpc.Make (struct
          let name = name

          include Callee
        end)

      let%test _ = Int.Set.equal (Caller.versions ()) (Callee.versions ())

      module type Version_shared = sig
        val version : int

        type query [@@deriving bin_io]
        type response [@@deriving bin_io]
        type error [@@deriving bin_io]

        val query_of_caller_model : Model.Caller.query -> query
        val callee_model_of_query : query -> Model.Callee.query
        val error_of_callee_model : Model.Callee.error -> error
        val caller_model_of_error : error -> Model.Caller.error
        val client_pushes_back : bool
      end

      module Register_raw (Version_i : sig
          include Version_shared

          val response_of_callee_model
            :  Model.Callee.response Pipe.Reader.t
            -> response Pipe.Reader.t

          val caller_model_of_response
            :  response Pipe.Reader.t
            -> Model.Caller.response Or_error.t Pipe.Reader.t
        end) =
      struct
        include Callee.Register_raw (struct
            include Version_i

            let model_of_query = callee_model_of_query
            let response_of_model = response_of_callee_model
            let error_of_model = error_of_callee_model
          end)

        include Caller.Register_raw (struct
            include Version_i

            let query_of_model = query_of_caller_model
            let model_of_response = caller_model_of_response
            let model_of_error = caller_model_of_error
          end)
      end

      module Register (Version_i : sig
          include Version_shared

          val response_of_callee_model : Model.Callee.response -> response
          val caller_model_of_response : response -> Model.Caller.response
        end) =
      struct
        include Callee.Register (struct
            include Version_i

            let model_of_query = callee_model_of_query
            let response_of_model = response_of_callee_model
            let error_of_model = error_of_callee_model
          end)

        include Caller.Register (struct
            include Version_i

            let query_of_model = query_of_caller_model
            let model_of_response = caller_model_of_response
            let model_of_error = caller_model_of_error
          end)
      end

      let dispatch_multi = Caller.dispatch_multi
      let dispatch_iter_multi = Caller.dispatch_iter_multi
      let abort_multi = Caller.abort_multi
      let implement_multi = Callee.implement_multi
      let implement_direct_multi = Callee.implement_direct_multi
      let versions () = Caller.versions ()
      let rpcs () = Caller.rpcs ()
    end
  end

  module State_rpc = struct
    module type S = sig
      type caller_query
      type callee_query
      type caller_state
      type callee_state
      type caller_update
      type callee_update
      type caller_error
      type callee_error

      val dispatch_multi
        :  Connection_with_menu.t
        -> caller_query
        -> ( caller_state * caller_update Or_error.t Pipe.Reader.t * State_rpc.Metadata.t
             , caller_error )
             Result.t
             Or_error.t
             Deferred.t

      val implement_multi
        :  ?log_not_previously_seen_version:(name:string -> int -> unit)
        -> ('state
            -> version:int
            -> callee_query
            -> (callee_state * callee_update Pipe.Reader.t, callee_error) Result.t
                 Deferred.t)
        -> 'state Implementation.t list

      val rpcs : unit -> Any.t list
      val versions : unit -> Int.Set.t
      val name : string
    end

    module Make (Model : sig
        val name : string

        module Caller : sig
          type query
          type state
          type update
          type error
        end

        module Callee : sig
          type query
          type state
          type update
          type error
        end
      end) =
    struct
      open Model

      let name = name

      module Caller = Caller_converts.State_rpc.Make (struct
          let name = name

          include Caller
        end)

      module Callee = Callee_converts.State_rpc.Make (struct
          let name = name

          include Callee
        end)

      let%test _ = Int.Set.equal (Caller.versions ()) (Callee.versions ())

      module type Version_shared = sig
        val version : int

        type query [@@deriving bin_io]
        type state [@@deriving bin_io]
        type update [@@deriving bin_io]
        type error [@@deriving bin_io]

        val query_of_caller_model : Model.Caller.query -> query
        val callee_model_of_query : query -> Model.Callee.query
        val caller_model_of_state : state -> Model.Caller.state
        val state_of_callee_model : Model.Callee.state -> state
        val caller_model_of_error : error -> Model.Caller.error
        val error_of_callee_model : Model.Callee.error -> error
        val client_pushes_back : bool
      end

      module Register_raw (Version_i : sig
          include Version_shared

          val caller_model_of_update
            :  update Pipe.Reader.t
            -> Model.Caller.update Or_error.t Pipe.Reader.t

          val update_of_callee_model
            :  Model.Callee.state
            -> Model.Callee.update Pipe.Reader.t
            -> update Pipe.Reader.t
        end) =
      struct
        include Callee.Register_raw (struct
            include Version_i

            let model_of_query = callee_model_of_query
            let state_of_model = state_of_callee_model
            let update_of_model = update_of_callee_model
            let error_of_model = error_of_callee_model
          end)

        include Caller.Register_raw (struct
            include Version_i

            let query_of_model = query_of_caller_model
            let model_of_state = caller_model_of_state
            let model_of_update = caller_model_of_update
            let model_of_error = caller_model_of_error
          end)
      end

      module Register (Version_i : sig
          include Version_shared

          val update_of_callee_model : Model.Callee.update -> update
          val caller_model_of_update : update -> Model.Caller.update
        end) =
      struct
        include Callee.Register (struct
            include Version_i

            let model_of_query = callee_model_of_query
            let state_of_model = state_of_callee_model
            let update_of_model = update_of_callee_model
            let error_of_model = error_of_callee_model
          end)

        include Caller.Register (struct
            include Version_i

            let query_of_model = query_of_caller_model
            let model_of_state = caller_model_of_state
            let model_of_update = caller_model_of_update
            let model_of_error = caller_model_of_error
          end)
      end

      let dispatch_multi = Caller.dispatch_multi
      let implement_multi = Callee.implement_multi
      let versions () = Caller.versions ()
      let rpcs () = Caller.rpcs ()
    end
  end

  module One_way = struct
    module type S = sig
      type caller_msg
      type callee_msg

      val dispatch_multi : Connection_with_menu.t -> caller_msg -> unit Or_error.t

      val implement_multi
        :  ?log_not_previously_seen_version:(name:string -> int -> unit)
        -> ('state -> version:int -> callee_msg -> unit)
        -> 'state Implementation.t list

      val rpcs : unit -> Any.t list
      val versions : unit -> Int.Set.t
      val name : string
    end

    module Make (Model : sig
        val name : string

        module Caller : sig
          type msg
        end

        module Callee : sig
          type msg
        end
      end) =
    struct
      open Model

      let name = name

      module Caller = Caller_converts.One_way.Make (struct
          let name = name

          include Caller
        end)

      module Callee = Callee_converts.One_way.Make (struct
          let name = name

          include Callee
        end)

      let%test _ = Int.Set.equal (Caller.versions ()) (Callee.versions ())

      module Register (Version : sig
          open Model

          val version : int

          type msg [@@deriving bin_io]

          val msg_of_caller_model : Caller.msg -> msg
          val callee_model_of_msg : msg -> Callee.msg
        end) =
      struct
        include Callee.Register (struct
            include Version

            let model_of_msg = callee_model_of_msg
          end)

        include Caller.Register (struct
            include Version

            let msg_of_model = msg_of_caller_model
          end)

        let%test _ = Int.Set.equal (Caller.versions ()) (Callee.versions ())
      end

      let dispatch_multi = Caller.dispatch_multi
      let implement_multi = Callee.implement_multi

      (* Note: Caller.versions is the same as Callee.versions, so it doesn't matter which
         one we call here. Same for [rpcs]. *)
      let versions () = Caller.versions ()
      let rpcs () = Caller.rpcs ()
    end
  end
end
