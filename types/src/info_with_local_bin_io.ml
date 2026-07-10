include Core.Info.Portable

let bin_size_t__local (t @ local) = bin_size_t ([%globalize: Core.Info.Portable.t] t)

let bin_write_t__local buf ~pos (t @ local) =
  bin_write_t buf ~pos ([%globalize: Core.Info.Portable.t] t)
;;
