include Core.Info

let bin_size_t__local t = bin_size_t ([%globalize: Core.Info.t] t)
let bin_write_t__local buf ~pos t = bin_write_t buf ~pos ([%globalize: Core.Info.t] t)
