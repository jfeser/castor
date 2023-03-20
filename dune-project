(lang dune 3.1)

(name castor)
(source (github jfeser/castor))

(license MIT)
(authors "Jack Feser <feser@csail.mit.edu>")
(maintainers "Jack Feser <feser@csail.mit.edu>")

(package
 (name castor)
 (synopsis "A deductive database synthesizer")
 (depends
  (dune (>= 1.10))
  (menhir :build)

  dune-site
  (core (>= v0.12))
  core_unix
  combinat
  ocamlgraph
  iter
  llvm
  (postgresql (>= 4.4))
  (bos (>= 0.2))
  (logs (>= 0.6))
  (hashcons (>= 1.3))
  (lwt (>= 4.2.1))
  (lwt_ppx (>= 1.2.2))
  (ppx_compare (>= v0.12))
  (ppx_sexp_conv (>= v0.12))
  (ppx_hash (>= v0.12))
  (ppx_let (>= v0.12))
  (visitors (>= 20180513))
  yojson)
 (sites (share code)))

(package
 (name castor_test)
 (synopsis "Tests for Castor")
 (depends
  (dune (and :build (>= 1.10)))

  castor
  (postgresql (>= 4.4))
  (bos (>= 0.2))
  (logs (>= 0.6))
  sexp_diff
  (hashcons (>= 1.3))
  (lwt (>= 4.2.1))
  (lwt_ppx (>= 1.2.2))
  (ppx_compare (>= v0.12))
  (ppx_sexp_conv (>= v0.12))
  (ppx_hash (>= v0.12))
  (ppx_let (>= v0.12))
  (visitors (>= 20180513))

  ounit
  (expect_test_helpers_core (>= v0.12))))

(package
 (name sqlgg)
 (synopsis "Just a SQL parser. Based on sqlgg")
 (depends
  (dune (and :build (>= 1.10)))

   menhir
   base
   stdio
   ppx_sexp_conv
   ppx_compare))

(package
 (name genhash)
 (synopsis "Generate hash functions")
 (depends
  (dune (and :build (>= 1.11)))
  (core (>= v0.12))
  (integers (>= 0.3))
  (ppx_sexp_conv (>= v0.12))
  (ppx_compare (>= v0.12))
  (ppx_expect (>= v0.12))))

(package
 (name cmph)
 (synopsis "OCaml wrapper for the C Minimal Perfect Hashing library")
 (description "ocaml-cmph is a wrapper library for the C Minimal Perfect Hashing (CMPH)
library. CMPH contains algorithms for generating perfect hashes. ocaml-cmph
exposes a high level interface to these hash generators.
")
 (depends
  (dune (and :build (>= 1.10)))
  (ounit (and :with_test (>= 2.0)))
  (ctypes (>= 0.14))
  (ctypes-foreign (>= 0.4))
  (core (>= v0.12))
  (ppx_sexp_conv (>= v0.12))))

(using menhir 2.0)
(using dune_site 0.1)
(generate_opam_files true)

; Local Variables:
; mode: dune
; End:
