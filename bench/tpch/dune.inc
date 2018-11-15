
(rule
  (targets "1-gold.gen")
  (deps "1.args" "1.txt" ../../bin/transform.exe)
  (action (ignore-stderr (with-stdout-to
            "1-gold.gen"
            (system "../../bin/transform.exe -db tpch_test 1.txt %{read:1.args}"))))
)
(alias
  (name check_transforms)
  (action (diff "1-gold.txt" "1-gold.gen"))
)
            

(rule
  (targets "2-gold.gen")
  (deps "2.args" "2.txt" ../../bin/transform.exe)
  (action (ignore-stderr (with-stdout-to
            "2-gold.gen"
            (system "../../bin/transform.exe -db tpch_test 2.txt %{read:2.args}"))))
)
(alias
  (name check_transforms)
  (action (diff "2-gold.txt" "2-gold.gen"))
)
            

(rule
  (targets "3-no-gold.gen")
  (deps "3-no.args" "3-no.txt" ../../bin/transform.exe)
  (action (ignore-stderr (with-stdout-to
            "3-no-gold.gen"
            (system "../../bin/transform.exe -db tpch_test 3-no.txt %{read:3-no.args}"))))
)
(alias
  (name check_transforms)
  (action (diff "3-no-gold.txt" "3-no-gold.gen"))
)
            

(rule
  (targets "4-gold.gen")
  (deps "4.args" "4.txt" ../../bin/transform.exe)
  (action (ignore-stderr (with-stdout-to
            "4-gold.gen"
            (system "../../bin/transform.exe -db tpch_test 4.txt %{read:4.args}"))))
)
(alias
  (name check_transforms)
  (action (diff "4-gold.txt" "4-gold.gen"))
)
            

(rule
  (targets "5-no-gold.gen")
  (deps "5-no.args" "5-no.txt" ../../bin/transform.exe)
  (action (ignore-stderr (with-stdout-to
            "5-no-gold.gen"
            (system "../../bin/transform.exe -db tpch_test 5-no.txt %{read:5-no.args}"))))
)
(alias
  (name check_transforms)
  (action (diff "5-no-gold.txt" "5-no-gold.gen"))
)
            

(rule
  (targets "6-gold.gen")
  (deps "6.args" "6.txt" ../../bin/transform.exe)
  (action (ignore-stderr (with-stdout-to
            "6-gold.gen"
            (system "../../bin/transform.exe -db tpch_test 6.txt %{read:6.args}"))))
)
(alias
  (name check_transforms)
  (action (diff "6-gold.txt" "6-gold.gen"))
)
            

(rule
  (targets "7-gold.gen")
  (deps "7.args" "7.txt" ../../bin/transform.exe)
  (action (ignore-stderr (with-stdout-to
            "7-gold.gen"
            (system "../../bin/transform.exe -db tpch_test 7.txt %{read:7.args}"))))
)
(alias
  (name check_transforms)
  (action (diff "7-gold.txt" "7-gold.gen"))
)
            

(rule
  (targets "8-gold.gen")
  (deps "8.args" "8.txt" ../../bin/transform.exe)
  (action (ignore-stderr (with-stdout-to
            "8-gold.gen"
            (system "../../bin/transform.exe -db tpch_test 8.txt %{read:8.args}"))))
)
(alias
  (name check_transforms)
  (action (diff "8-gold.txt" "8-gold.gen"))
)
            

(rule
  (targets "9-gold.gen")
  (deps "9.args" "9.txt" ../../bin/transform.exe)
  (action (ignore-stderr (with-stdout-to
            "9-gold.gen"
            (system "../../bin/transform.exe -db tpch_test 9.txt %{read:9.args}"))))
)
(alias
  (name check_transforms)
  (action (diff "9-gold.txt" "9-gold.gen"))
)
            

(rule
  (targets "10-no-gold.gen")
  (deps "10-no.args" "10-no.txt" ../../bin/transform.exe)
  (action (ignore-stderr (with-stdout-to
            "10-no-gold.gen"
            (system "../../bin/transform.exe -db tpch_test 10-no.txt %{read:10-no.args}"))))
)
(alias
  (name check_transforms)
  (action (diff "10-no-gold.txt" "10-no-gold.gen"))
)
            

(rule
  (targets "11-no-gold.gen")
  (deps "11-no.args" "11-no.txt" ../../bin/transform.exe)
  (action (ignore-stderr (with-stdout-to
            "11-no-gold.gen"
            (system "../../bin/transform.exe -db tpch_test 11-no.txt %{read:11-no.args}"))))
)
(alias
  (name check_transforms)
  (action (diff "11-no-gold.txt" "11-no-gold.gen"))
)
            

(rule
  (targets "12-gold.gen")
  (deps "12.args" "12.txt" ../../bin/transform.exe)
  (action (ignore-stderr (with-stdout-to
            "12-gold.gen"
            (system "../../bin/transform.exe -db tpch_test 12.txt %{read:12.args}"))))
)
(alias
  (name check_transforms)
  (action (diff "12-gold.txt" "12-gold.gen"))
)
            

(rule
  (targets "14-gold.gen")
  (deps "14.args" "14.txt" ../../bin/transform.exe)
  (action (ignore-stderr (with-stdout-to
            "14-gold.gen"
            (system "../../bin/transform.exe -db tpch_test 14.txt %{read:14.args}"))))
)
(alias
  (name check_transforms)
  (action (diff "14-gold.txt" "14-gold.gen"))
)
            

(rule
  (targets "15-gold.gen")
  (deps "15.args" "15.txt" ../../bin/transform.exe)
  (action (ignore-stderr (with-stdout-to
            "15-gold.gen"
            (system "../../bin/transform.exe -db tpch_test 15.txt %{read:15.args}"))))
)
(alias
  (name check_transforms)
  (action (diff "15-gold.txt" "15-gold.gen"))
)
            

(rule
  (targets "16-no-gold.gen")
  (deps "16-no.args" "16-no.txt" ../../bin/transform.exe)
  (action (ignore-stderr (with-stdout-to
            "16-no-gold.gen"
            (system "../../bin/transform.exe -db tpch_test 16-no.txt %{read:16-no.args}"))))
)
(alias
  (name check_transforms)
  (action (diff "16-no-gold.txt" "16-no-gold.gen"))
)
            

(rule
  (targets "17-gold.gen")
  (deps "17.args" "17.txt" ../../bin/transform.exe)
  (action (ignore-stderr (with-stdout-to
            "17-gold.gen"
            (system "../../bin/transform.exe -db tpch_test 17.txt %{read:17.args}"))))
)
(alias
  (name check_transforms)
  (action (diff "17-gold.txt" "17-gold.gen"))
)
            

(rule
  (targets "18-gold.gen")
  (deps "18.args" "18.txt" ../../bin/transform.exe)
  (action (ignore-stderr (with-stdout-to
            "18-gold.gen"
            (system "../../bin/transform.exe -db tpch_test 18.txt %{read:18.args}"))))
)
(alias
  (name check_transforms)
  (action (diff "18-gold.txt" "18-gold.gen"))
)
            

(rule
  (targets "19-gold.gen")
  (deps "19.args" "19.txt" ../../bin/transform.exe)
  (action (ignore-stderr (with-stdout-to
            "19-gold.gen"
            (system "../../bin/transform.exe -db tpch_test 19.txt %{read:19.args}"))))
)
(alias
  (name check_transforms)
  (action (diff "19-gold.txt" "19-gold.gen"))
)
            
