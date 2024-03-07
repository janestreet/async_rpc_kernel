open Core

type _ t =
  | Only_on_exn : _ t
  | Or_error : _ Or_error.t t
  | Result : (_, _) Result.t t
  | Or_error_or_error : _ Or_error.t Or_error.t t
  | Result_or_error : (_, _) Result.t Or_error.t t
  | Or_error_result : (_ Or_error.t, _) Result.t t
  | Result_result : ((_, _) Result.t, _) Result.t t
  | On_none : _ Option.t t
  | Generic_or_error : 'inner t -> 'inner Or_error.t t
  | Generic_result : 'inner t -> ('inner, _) Result.t t
  | Generic_option : 'inner t -> 'inner Option.t t
  | Embedded : ('a -> 'b) * 'b t -> 'a t
  | Custom : { is_error : 'a -> bool } -> 'a t

module Private = struct
  let[@inline] rec as_function : 'r. 'r t -> ('r -> bool) Staged.t =
    fun (type r) (errors : r t) : (r -> bool) Staged.t ->
    (match errors with
     | Only_on_exn -> stage (fun _ -> false)
     | Or_error -> stage Result.is_error
     | Result -> stage Result.is_error
     | Or_error_or_error -> as_function Result_result
     | Result_or_error -> as_function Result_result
     | Or_error_result -> as_function Result_result
     | Result_result ->
       stage (function
         | Ok (Ok _) -> false
         | _ -> true)
     | Generic_or_error t -> as_function (Generic_result t)
     | Generic_result t ->
       let f = unstage (as_function t) in
       stage (function
         | Error _ -> true
         | Ok x -> f x)
     | On_none ->
       stage (function
         | None -> true
         | _ -> false)
     | Generic_option t ->
       let f = unstage (as_function t) in
       stage (function
         | None -> true
         | Some x -> f x)
     | Embedded (extract, t) ->
       let f = unstage (as_function t) in
       stage (fun x -> f (extract x))
     | Custom { is_error } -> stage is_error)
  ;;

  let[@inline] to_error_mode (type r) (errors : r t) : r Implementation.F.error_mode =
    match errors with
    | Only_on_exn -> Always_ok
    | Or_error -> Using_result
    | Result -> Using_result
    | Or_error_or_error -> Using_result_result
    | Result_or_error -> Using_result_result
    | Or_error_result -> Using_result_result
    | Result_result -> Using_result_result
    | On_none | Generic_or_error _ | Generic_result _ | Generic_option _
    | Embedded (_, _)
    | Custom _ -> Is_error (unstage (as_function errors))
  ;;
end
