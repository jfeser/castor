(lang dune 1.10)

(name castor)
(source (github jfeser/castor))
(license MIT)
(authors "Jack Feser <feser@csail.mit.edu>")
(maintainers "Jack Feser <feser@csail.mit.edu>")

(package
  (name castor)
  (synopsis "A deductive database synthesizer")
  (depends
    (dune (and :build (>= 1.10)))
    (menhir :build)

    (core (>= v0.12))
    (postgresql (>= 4.4))
    (bos (>= 0.2))
    (logs (>= 0.6))
    (llvm (and (>= 7.0.0) (< 8.0.0)))
    (gen (>= 0.5))
     cmph
     genhash
    (hashcons (>= 1.3))
    (lwt (>= 4.2.1))
    (lwt_ppx (>= 1.2.2))
    (ppx_compare (>= v0.12))
    (ppx_sexp_conv (>= v0.12))
    (ppx_hash (>= v0.12))
    (ppx_let (>= v0.12))
    (visitors (>= 20180513))))

(package
 (name castor_test)
 (synopsis "Tests for Castor")
 (depends
  (dune (and :build (>= 1.10)))

  castor
  (postgresql (>= 4.4))
  (bos (>= 0.2))
  (logs (>= 0.6))
  (llvm (and (>= 7.0.0) (< 8.0.0)))
  (gen (>= 0.5))
   cmph
   genhash
  (hashcons (>= 1.3))
  (lwt (>= 4.2.1))
  (lwt_ppx (>= 1.2.2))
  (ppx_compare (>= v0.12))
  (ppx_sexp_conv (>= v0.12))
  (ppx_hash (>= v0.12))
  (ppx_let (>= v0.12))
  (visitors (>= 20180513))

  ounit
  (expect_test_helpers_kernel (>= v0.12))))


(using fmt 1.1)
(using menhir 2.0)
(generate_opam_files true)

; Local Variables:
; mode: dune
; End:
