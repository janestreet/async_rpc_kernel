open Core

module Stable = struct
  open Core.Core_stable

  module V1 = struct
    module T = struct
      type t =
        { global_ name : string
        ; version : int
        }
      [@@deriving
        bin_io ~localize
        , equal ~localize
        , globalize
        , compare ~localize
        , hash
        , sexp
        , globalize
        , stable_witness]

      let bin_read_t__local buf ~pos_ref = exclave_
        let name = bin_read_string buf ~pos_ref in
        let version = bin_read_int buf ~pos_ref in
        { name; version }
      ;;

      let%expect_test _ =
        print_endline [%bin_digest: t];
        [%expect {| 4521f44dbc6098c0afc2770cc84552b1 |}]
      ;;

      include (val Comparator.V1.make ~compare ~sexp_of_t)
    end

    include T
    include Comparable.V1.Make (T)
  end
end

include Stable.V1
include Hashable.Make (Stable.V1)
include Comparable.Make_using_comparator (Stable.V1)

let to_alist ts = List.map ts ~f:(fun { name; version } -> name, version)
let of_alist list = List.map list ~f:(fun (name, version) -> { name; version })

let summarize ts =
  to_alist ts |> String.Map.of_alist_fold ~init:Int.Set.empty ~f:Core.Set.add
;;

let%expect_test _ =
  let descriptions =
    [ { name = "foo"; version = 1 }
    ; { name = "foo"; version = 2 }
    ; { name = "bar"; version = 5 }
    ]
  in
  let summary = summarize descriptions in
  print_s [%sexp (summary : Int.Set.t String.Map.t)];
  [%expect {| ((bar (5)) (foo (1 2))) |}]
;;
