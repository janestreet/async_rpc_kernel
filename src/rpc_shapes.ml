module Stable = struct
  open! Core.Core_stable

  (* We can't make [Bin_prot.Shape.t] binable directly because that would introduce a
     circular dependency, so we make it [Binable.Of_sexpable_with_uuid] here instead. This
     allows us to send the shape over the wire as part of the
     [Versioned_rpc.Shape_menu.t]. [Bin_prot.Shape.Canonical.t], though exposed and
     therefore binable, is not a suitable alternative because it has an exponential
     expansion. *)
  module Shape = struct
    module V1 = struct
      module T = struct
        include Bin_prot.Shape.Stable.V1

        let caller_identity =
          Bin_prot.Shape.Uuid.of_string "0aa53549-ad88-4b4f-abc7-1f8453e7aa11"
        ;;
      end

      include T
      include Binable.Of_sexpable.V2 (T)
    end
  end

  module V1 = struct
    type t =
      | Rpc of
          { query : Shape.V1.t
          ; response : Shape.V1.t
          }
      | One_way of { msg : Shape.V1.t }
      | Streaming_rpc of
          { query : Shape.V1.t
          ; initial_response : Shape.V1.t
          ; update_response : Shape.V1.t
          ; error : Shape.V1.t
          }
      | Unknown
    [@@deriving bin_io, sexp]
  end
end

open! Core

module Shape = struct
  type t = Bin_prot.Shape.t

  (* An unstable sexper that's useful for expect tests, printing out the shape digests
     instead of the shapes themselves. *)
  let sexp_of_t t = [%sexp (Bin_prot.Shape.eval_to_digest_string t : string)]
end

type t = Stable.V1.t =
  | Rpc of
      { query : Shape.t
      ; response : Shape.t
      }
  | One_way of { msg : Shape.t }
  | Streaming_rpc of
      { query : Shape.t
      ; initial_response : Shape.t
      ; update_response : Shape.t
      ; error : Shape.t
      }
  | Unknown
[@@deriving sexp_of]
