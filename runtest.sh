#!/bin/sh

dune build ../_build/default/castor/test/.castor_test.inline-tests/run.exe
cd ..
_build/default/castor/test/.castor_test.inline-tests/run.exe inline-test-runner castor_test -only-test $1
