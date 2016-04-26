#use "topfind";;
#require "js-build-tools.oasis2opam_install";;

open Oasis2opam_install;;

generate ~package:"async_rpc_kernel"
  [ oasis_lib "async_rpc_kernel"
  ; file "META" ~section:"lib"
  ]
