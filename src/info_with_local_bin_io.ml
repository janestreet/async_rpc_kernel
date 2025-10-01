include Core.Info

let bin_size_t__local (local_ t) = bin_size_t ([%globalize: Core.Info.t] t)

let bin_write_t__local buf ~pos (local_ t) =
  bin_write_t buf ~pos ([%globalize: Core.Info.t] t)
;;
