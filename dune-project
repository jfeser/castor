(lang dune 1.10)

(name castor-opt)
(source (github jfeser/castor-opt))
(license MIT)
(authors "Jack Feser <feser@csail.mit.edu>")
(maintainers "Jack Feser <feser@csail.mit.edu>")

(package
  (name castor-opt)
  (synopsis "Optimizer for the Castor synthesizer")
  (depends
    (dune (and :build (>= 1.10)))

    castor
    (core (>= v0.12))
    (cmdliner (>= 1.0))
    (logs (>= 0.6))
    (fmt (>= 0.8.5))
    (yojson (>= 1.7))
    (ocamlgraph (>= 1.8.8))
    (combinat (>= 1.0))
    (ppx_compare (>= v0.12))
    (ppx_sexp_conv (>= v0.12))
    (ppx_hash (>= v0.12))
    (ppx_let (>= v0.12))

    (expect_test_helpers_kernel (and :with-test (>= v0.12)))))

(using fmt 1.1)
(generate_opam_files true)

; Local Variables:
; mode: dune
; End:
