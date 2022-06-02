
(rule
 (alias runtest)
 (action (diff elim-cmp-filter.sexp.expected elim-cmp-filter.sexp.output)))

(rule
 (alias runtest)
 (action (with-stdout-to elim-cmp-filter.sexp.output
          (with-stdin-from elim-cmp-filter.sexp
           (run ../bin/eopt.exe)))))


(rule
 (alias runtest)
 (action (diff hoist-filter-select.sexp.expected hoist-filter-select.sexp.output)))

(rule
 (alias runtest)
 (action (with-stdout-to hoist-filter-select.sexp.output
          (with-stdin-from hoist-filter-select.sexp
           (run ../bin/eopt.exe)))))


(rule
 (alias runtest)
 (action (diff split-filter.sexp.expected split-filter.sexp.output)))

(rule
 (alias runtest)
 (action (with-stdout-to split-filter.sexp.output
          (with-stdin-from split-filter.sexp
           (run ../bin/eopt.exe)))))

