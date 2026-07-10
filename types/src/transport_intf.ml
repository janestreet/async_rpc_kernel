open Core

module Send_result = struct
  type message_too_big =
    { size : int
    ; max_message_size : int
    }
  [@@deriving bin_io ~localize, compare ~localize, globalize, sexp]

  let bin_read_message_too_big__local (buf @ local) ~(pos_ref @ local) =
    let size = bin_read_int buf ~pos_ref in
    let max_message_size = bin_read_int buf ~pos_ref in
    exclave_ { size; max_message_size }
  ;;

  type 'a t =
    | Sent of
        { result : 'a @@ global
        ; bytes : int
        (** Bytes should equal the size of the bin_prot rpc message and data. The total
            bytes written on the network in the standard protocol (which has 8-bytes sizes
            before each frame) will be [sum([8 + x.bytes for each send result x])]. Other
            framing protocols or encryption (e.g. rpc over kerberos) may write more or
            less bytes. *)
        }
    | Closed
    | Message_too_big of message_too_big
  [@@deriving compare ~localize, globalize, sexp_of]
end
