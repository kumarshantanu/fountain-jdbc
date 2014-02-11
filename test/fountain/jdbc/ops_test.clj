(ns fountain.jdbc.ops-test
  (:require [fountain.jdbc           :as j]
            [fountain.jdbc.factory   :as f]
            [fountain.jdbc.impl-ops  :as i]
            [fountain.jdbc.test-util :as tu]
            [clj-dbcp.core           :as cp]
            [clojure.test :refer :all]))


(def dbspec {:datasource (cp/make-datasource :h2 {:target :memory :database "emp"
                                                  :user "sa" :password ""})})


(def sql-coll
  ["INSERT INTO sample (name, age, gender, is_emp) VALUES ('David', 50, 'M', false)"
   "INSERT INTO sample (name, age, gender, is_emp) VALUES ('Helga', 60, 'F', false)"
   "UPDATE sample SET age=50 WHERE name='Helga'"])


(deftest test-batch-update
  (testing "no args"
           (tu/with-sample dbspec ops npops
              (is (= [1 1 1] (j/batch-update ops sql-coll)))
              (is (= 2 (j/query-for-val ops "SELECT COUNT(*) FROM sample WHERE age = 50")))))
  (testing "with args"
           (tu/with-sample dbspec ops npops
             (is (= [1 1]
                    (j/batch-update
                      ops "INSERT INTO sample (name, age, gender, is_emp) VALUES (?, ?, ?, ?)"
                      [["David" 50 "M" false]
                       ["Helga" 60 "F" false]])))
             (is (= 2 (j/query-for-val ops "SELECT COUNT(*) FROM sample WHERE is_emp = false")))))
  (testing "with argmap"
           (tu/with-sample dbspec ops npops
             (is (= [1 1]
                    (j/batch-update
                      npops
                      "INSERT INTO sample (name, age, gender, is_emp) VALUES (:name, :age, :gender, :is_emp)"
                      [{:name "David" :age 50 :gender "M" :is-emp false}
                       {:name "Helga" :age 60 :gender "F" :is-emp false}])))
             (is (= 2 (j/query-for-val ops "SELECT COUNT(*) FROM sample WHERE is_emp = false"))))))


(deftest test-execute
  (tu/with-sample dbspec ops npops
    (testing "ops"
             (j/execute ops "CREATE TABLE foo (bar INT)")
             (j/execute ops "INSERT INTO foo (bar) VALUES (50)")
             (is (= 50 (j/query-for-val ops "SELECT * FROM foo")))
             (j/execute ops "DROP TABLE foo"))
    (testing "named ops"
             (j/execute npops "CREATE TABLE foo (bar INT)")
             (j/execute npops "INSERT INTO foo (bar) VALUES (50)")
             (is (= 50 (j/query-for-val npops "SELECT * FROM foo")))
             (j/execute npops "DROP TABLE foo"))))


(defn run-lazy-query-tests
  [ops npops]
  (testing "no args"
           (is (= [[:age :gender] [[30 "M"] [40 "F"]]]
                  (j/lazy-query ops (fn ^:once [colnames dataseq]
                                      [colnames (doall dataseq)])
                                true "SELECT age, gender FROM sample WHERE is_emp = true")))
           (is (= [[:age :gender] [[30 "M"] [40 "F"]]]
                  (j/lazy-query npops (fn ^:once [colnames dataseq]
                                        [colnames (doall dataseq)])
                                true "SELECT age, gender FROM sample WHERE is_emp = true"))))
  (testing "with args"
           (is (= [[:age :gender] [[30 "M"] [40 "F"]]]
                  (j/lazy-query ops (fn ^:once [colnames dataseq]
                                      [colnames (doall dataseq)])
                                true "SELECT age, gender FROM sample WHERE is_emp = ?" [true])))
           (is (= [[:age :gender] [[30 "M"] [40 "F"]]]
                  (j/lazy-query npops (fn ^:once [colnames dataseq]
                                        [colnames (doall dataseq)])
                                true "SELECT age, gender FROM sample WHERE is_emp = :is_emp"
                                {:is-emp true})))))


(deftest test-lazy-query
  (tu/with-sample dbspec ops npops
    (run-lazy-query-tests ops npops)))


(defn run-query-datalist-tests
  [ops npops]
  (testing "no args"
           (is (= [[] [[30 "M"] [40 "F"]]]
                  (j/query-datalist ops "SELECT age, gender FROM sample WHERE is_emp = true")))
           (is (= [[] [[30 "M"] [40 "F"]]]
                  (j/query-datalist npops "SELECT age, gender FROM sample WHERE is_emp = true"))))
  (testing "with args"
           (is (= [[] [[30 "M"] [40 "F"]]]
                  (j/query-datalist ops "SELECT age, gender FROM sample WHERE is_emp = ?" [true])))
           (is (= [[] [[30 "M"] [40 "F"]]]
                  (j/query-datalist npops "SELECT age, gender FROM sample WHERE is_emp = :is_emp"
                                    {:is-emp true})))))


(deftest test-query-datalist
  (tu/with-sample dbspec ops npops
    (run-query-datalist-tests ops npops)))


(defn run-query-datarow-tests
  [ops npops]
  (testing "no args"
           (is (= [[] [30]] (j/query-datarow ops "SELECT age FROM sample WHERE age = 30")))
           (is (= [[] [30]] (j/query-datarow npops "SELECT age FROM sample WHERE age = 30"))))
  (testing "with args"
           (is (= [[] [30]] (j/query-datarow ops "SELECT age FROM sample WHERE age = ?" [30])))
           (is (= [[] [30]] (j/query-datarow npops "SELECT age FROM sample WHERE age = :age"
                                             {:age 30})))))


(deftest test-query-datarow
  (tu/with-sample dbspec ops npops
    (run-query-datarow-tests ops npops)))


(defn run-query-for-list-tests
  [ops npops]
  (testing "no args"
           (is (= [[:age :gender] [[30 "M"] [40 "F"]]]
                  (j/query-for-list ops "SELECT age, gender FROM sample WHERE is_emp = true")))
           (is (= [[:age :gender] [[30 "M"] [40 "F"]]]
                  (j/query-for-list npops "SELECT age, gender FROM sample WHERE is_emp = true"))))
  (testing "with args"
           (is (= [[:age :gender] [[30 "M"] [40 "F"]]]
                  (j/query-for-list ops "SELECT age, gender FROM sample WHERE is_emp = ?" [true])))
           (is (= [[:age :gender] [[30 "M"] [40 "F"]]]
                  (j/query-for-list npops "SELECT age, gender FROM sample WHERE is_emp = :is_emp"
                                    {:is_emp true})))))


(deftest test-query-for-list
  (tu/with-sample dbspec ops npops
    (run-query-for-list-tests ops npops)))


(defn run-query-for-row-tests
  [ops npops]
  (testing "no args"
           (is (= [[:age] [30]] (j/query-for-row ops "SELECT age FROM sample WHERE age = 30")))
           (is (= [[:age] [30]] (j/query-for-row npops "SELECT age FROM sample WHERE age = 30"))))
  (testing "with args"
           (is (= [[:age] [30]] (j/query-for-row ops "SELECT age FROM sample WHERE age = ?" [30])))
           (is (= [[:age] [30]] (j/query-for-row npops "SELECT age FROM sample WHERE age = :age"
                                                 {:age 30})))))


(deftest test-query-for-row
  (tu/with-sample dbspec ops npops
    (run-query-for-row-tests ops npops)))


(defn run-query-for-val-tests
  [ops npops]
  (testing "no args"
           (is (= 30 (j/query-for-val ops "SELECT age FROM sample WHERE age = 30")))
           (is (= 30 (j/query-for-val npops "SELECT age FROM sample WHERE age = 30"))))
  (testing "with args"
           (is (= 30 (j/query-for-val ops "SELECT age FROM sample WHERE age = ?" [30])))
           (is (= 30 (j/query-for-val npops "SELECT age FROM sample WHERE age = :age"
                                      {:age 30})))))


(deftest test-query-for-val
  (tu/with-sample dbspec ops npops
    (run-query-for-val-tests ops npops)))


(def update-sql-coll
  ["INSERT INTO sample (name, age, gender, is_emp) VALUES ('Maria', 24, 'F', true)"
   "INSERT INTO sample (name, age, gender, is_emp) VALUES ('Abbey', 21, 'M', true)"
   "UPDATE sample SET age = 24 WHERE name = 'Abbey'"])
(def update-args-coll
  [["INSERT INTO sample (name, age, gender, is_emp) VALUES (?, ?, ?, ?)" ["Maria", 24, "F", true]]
   ["INSERT INTO sample (name, age, gender, is_emp) VALUES (?, ?, ?, ?)" ["Abbey", 21, "M", true]]
   ["UPDATE sample SET age = ? WHERE name = ?" [24, "Abbey"]]])
(def update-argmap-coll
  [["INSERT INTO sample (name, age, gender, is_emp) VALUES (:name, :age, :gender, :is_emp)"
    {:name "Maria" :age 24 :gender "F" :is-emp true}]
   ["INSERT INTO sample (name, age, gender, is_emp) VALUES (:name, :age, :gender, :is_emp)"
    {:name "Abbey" :age 21 :gender "M" :is-emp true}]
   ["UPDATE sample SET age = :age WHERE name = :name" {:age 24 :name "Abbey"}]])

(def update-detect-sql   "SELECT COUNT(*) FROM sample WHERE age = 24")
(def update-teardown-sql "DELETE FROM sample WHERE age = 24")


(defmacro with-update
  [ops & body]
  `(do ~@body
     (is (= 2 (j/query-for-val ~ops update-detect-sql)))
     (j/update ~ops update-teardown-sql)))


(deftest test-update
  (tu/with-sample dbspec ops npops
    (testing "no args"
             (with-update ops
               (doseq [each update-sql-coll]
                 (is (= 1 (j/update ops each)))))
             (with-update ops
               (doseq [each update-sql-coll]
                 (is (= 1 (j/update npops each))))))
    (testing "with args"
             (with-update ops
               (doseq [[sql args] update-args-coll]
                 (is (= 1 (j/update ops sql args)))))
             (with-update ops
               (doseq [[sql args] update-argmap-coll]
                 (is (= 1 (j/update npops sql args))))))))


(defmacro with-sample2
  [dbspec ops npops & body]
  `(do
     (tu/sample2-setup ~dbspec)
     (let [~ops   (f/make-ops-impl ~dbspec)
           ~npops (f/make-ops-impl (assoc dbspec :named-params? true))]
       ~@body)))


(deftest test-genkey
  (testing "no args"
           (with-sample2 dbspec ops npops
             (let [last-id (j/query-for-val ops "SELECT MAX(id) FROM sample2")]
               (is (= (inc (or last-id 0))
                      (j/genkey ops "INSERT INTO sample2 (aux_id, name) VALUES (2, 'Kiran')")))))
           (with-sample2 dbspec ops npops
             (let [last-id (j/query-for-val npops "SELECT MAX(id) FROM sample2")]
               (is (= (inc (or last-id 0))
                      (j/genkey npops "INSERT INTO sample2 (aux_id, name) VALUES (2, 'Kiran')"))))))
  (testing "with args"
           (with-sample2 dbspec ops npops
             (let [last-id (j/query-for-val ops "SELECT MAX(id) FROM sample2")]
               (is (= (inc (or last-id 0))
                      (j/genkey ops "INSERT INTO sample2 (aux_id, name) VALUES (?, ?)"
                                [2, "Kiran"])))))
           (with-sample2 dbspec ops npops
            (let [last-id (j/query-for-val npops "SELECT MAX(id) FROM sample2")]
              (is (= (inc (or last-id 0))
                     (j/genkey npops "INSERT INTO sample2 (aux_id, name) VALUES (:aux_id, :name)"
                               {:aux-id 2 :name "Kiran"})))))))


(deftest test-genkey-via
  (testing "no args - get-keymap"
           (with-sample2 dbspec ops npops
             (let [[_ [last-id last-aux]]
                   (j/query-datarow ops "SELECT MAX(id), MAX(aux_id) FROM sample2")]
               (is (= {(keyword "scope-identity()") (inc (or last-id 0))}
                      (j/genkey-via ops j/get-keys
                                    "INSERT INTO sample2 (name) VALUES ('Kiran')")))))
           (with-sample2 dbspec ops npops
             (let [[_ [last-id last-aux]]
                   (j/query-datarow npops "SELECT MAX(id), MAX(aux_id) FROM sample2")]
               (is (= {(keyword "scope-identity()") (inc (or last-id 0))}
                      (j/genkey-via npops j/get-keys
                                    "INSERT INTO sample2 (name) VALUES ('Kiran')"))))))
  (testing "no args - get-keylist"
           (with-sample2 dbspec ops npops
             (let [[_ [last-id last-aux]]
                   (j/query-datarow ops "SELECT MAX(id), MAX(aux_id) FROM sample2")]
               (is (= [{(keyword "scope-identity()") (inc (or last-id 0))}]
                      (j/genkey-via ops j/get-keylist
                                    "INSERT INTO sample2 (name) VALUES ('Kiran')")))))
           (with-sample2 dbspec ops npops
             (let [[_ [last-id last-aux]]
                   (j/query-datarow npops "SELECT MAX(id), MAX(aux_id) FROM sample2")]
               (is (= [{(keyword "scope-identity()") (inc (or last-id 0))}]
                      (j/genkey-via npops j/get-keylist
                                    "INSERT INTO sample2 (name) VALUES ('Kiran')"))))))
  (testing "with args - get-keymap"
           (with-sample2 dbspec ops npops
             (let [[_ [last-id last-aux]]
                   (j/query-datarow ops "SELECT MAX(id), MAX(aux_id) FROM sample2")]
               (is (= {(keyword "scope-identity()") (inc (or last-id 0))}
                      (j/genkey-via ops j/get-keys
                                    "INSERT INTO sample2 (name) VALUES (?)" ["Kiran"])))))
           (with-sample2 dbspec ops npops
             (let [[_ [last-id last-aux]]
                   (j/query-datarow npops "SELECT MAX(id), MAX(aux_id) FROM sample2")]
               (is (= {(keyword "scope-identity()") (inc (or last-id 0))}
                      (j/genkey-via npops j/get-keys
                                    "INSERT INTO sample2 (name) VALUES (:name)"
                                    {:name "Kiran"}))))))
  (testing "with args - get-keylist"
           (with-sample2 dbspec ops npops
             (let [[_ [last-id last-aux]]
                   (j/query-datarow ops "SELECT MAX(id), MAX(aux_id) FROM sample2")]
               (is (= [{(keyword "scope-identity()") (inc (or last-id 0))}]
                      (j/genkey-via ops j/get-keylist
                                    "INSERT INTO sample2 (name) VALUES (?)" ["Kiran"])))))
           (with-sample2 dbspec ops npops
             (let [[_ [last-id last-aux]]
                   (j/query-datarow npops "SELECT MAX(id), MAX(aux_id) FROM sample2")]
               (is (= [{(keyword "scope-identity()") (inc (or last-id 0))}]
                      (j/genkey-via npops j/get-keylist
                                    "INSERT INTO sample2 (name) VALUES (:name)"
                                    {:name "Kiran"})))))))


(defmacro is-unsupported?
  [& body]
  `(is (~'thrown? UnsupportedOperationException ~@body)))


(deftest test-wrapper-readonly
  (tu/with-sample (assoc dbspec :read-only? true) ops npops
    (testing "batch-update"
             (is-unsupported? (j/batch-update ops sql-coll))
             (is-unsupported? (j/batch-update npops sql-coll))
             (is-unsupported? (j/batch-update ops "foo" [30]))
             (is-unsupported? (j/batch-update npops ":foo" {:foo 30}))
             (is-unsupported? (j/execute ops "foo"))
             (is-unsupported? (j/execute npops "foo"))
             (run-lazy-query-tests ops npops)
             (run-query-datalist-tests ops npops)
             (run-query-datarow-tests ops npops)
             (run-query-for-list-tests ops npops)
             (run-query-for-row-tests ops npops)
             (run-query-for-val-tests ops npops)
             (is-unsupported? (j/update ops "foo"))
             (is-unsupported? (j/update npops "foo"))
             (is-unsupported? (j/update ops "foo" [30]))
             (is-unsupported? (j/update npops "foo" {:foo 30}))
             (is-unsupported? (j/genkey ops "foo"))
             (is-unsupported? (j/genkey npops "foo"))
             (is-unsupported? (j/genkey ops "foo" [30]))
             (is-unsupported? (j/genkey npops "foo" {:foo 30}))
             (is-unsupported? (j/genkey-via ops identity "foo"))
             (is-unsupported? (j/genkey-via npops identity "foo"))
             (is-unsupported? (j/genkey-via ops identity "foo" [30]))
             (is-unsupported? (j/genkey-via npops identity "foo" {:foo 30})))))


(deftest test-mock-ops
  (let [mops (i/mock-ops-impl (constantly 1))]
    (is (= 1 (j/batch-update mops sql-coll)))
    (is (= 1 (j/batch-update mops "foo" [])))
    (is (= 1 (j/execute mops "foo")))
    (is (= 1 (j/lazy-query mops identity true "foo")))
    (is (= 1 (j/lazy-query mops identity true "foo" [30])))
    (is (= 1 (j/query-datalist mops "foo")))
    (is (= 1 (j/query-datalist mops "foo" [30])))
    (is (= 1 (j/query-datarow  mops "foo")))
    (is (= 1 (j/query-datarow  mops "foo" [30])))
    (is (= 1 (j/query-for-list mops "foo")))
    (is (= 1 (j/query-for-list mops "foo" [30])))
    (is (= 1 (j/query-for-row  mops "foo")))
    (is (= 1 (j/query-for-row  mops "foo" [30])))
    (is (= 1 (j/query-for-val  mops "foo")))
    (is (= 1 (j/query-for-val  mops "foo" [30])))
    (is (= 1 (j/update  mops "foo")))
    (is (= 1 (j/update  mops "foo" [30])))
    (is (= 1 (j/genkey  mops "foo")))
    (is (= 1 (j/genkey  mops "foo" [30])))
    (is (= 1 (j/genkey-via  mops identity "foo")))
    (is (= 1 (j/genkey-via  mops identity "foo" [30])))))


(defmacro is-before
  [& body]
  `(is (= "before1" (with-out-str ~@body))))


(deftest test-wrapper-before
  (let [ops (-> #(print 1)
              i/mock-ops-impl
              (i/wrap-ops-before #(print %)))]
    (is-before (j/batch-update ops "before"))
    (is-before (j/batch-update ops "before" [30]))
    (is-before (j/execute ops "before"))
    (is-before (j/lazy-query ops identity true "before"))
    (is-before (j/lazy-query ops identity true "before" [30]))
    (is-before (j/query-datalist ops "before"))
    (is-before (j/query-datalist ops "before" [30]))
    (is-before (j/query-datarow ops "before"))
    (is-before (j/query-datarow ops "before" [30]))
    (is-before (j/query-for-list ops "before"))
    (is-before (j/query-for-list ops "before" [30]))
    (is-before (j/query-for-row ops "before"))
    (is-before (j/query-for-row ops "before" [30]))
    (is-before (j/query-for-val ops "before"))
    (is-before (j/query-for-val ops "before" [30]))
    (is-before (j/update ops "before"))
    (is-before (j/update ops "before" [30]))
    (is-before (j/genkey ops "before"))
    (is-before (j/genkey ops "before" [30]))
    (is-before (j/genkey-via ops identity "before"))
    (is-before (j/genkey-via ops identity "before" [30]))))


(defmacro is-after
  [& body]
  `(is (re-matches #"1after \d+" (with-out-str ~@body))))


(deftest test-wrapper-after
  (let [ops (-> #(print 1)
              i/mock-ops-impl
              (i/wrap-ops-after (fn [sql elapsed] (print sql elapsed))))]
    (is-after (j/batch-update ops "after"))
    (is-after (j/batch-update ops "after" [30]))
    (is-after (j/execute ops "after"))
    (is-after (j/lazy-query ops identity true "after"))
    (is-after (j/lazy-query ops identity true "after" [30]))
    (is-after (j/query-datalist ops "after"))
    (is-after (j/query-datalist ops "after" [30]))
    (is-after (j/query-datarow ops "after"))
    (is-after (j/query-datarow ops "after" [30]))
    (is-after (j/query-for-list ops "after"))
    (is-after (j/query-for-list ops "after" [30]))
    (is-after (j/query-for-row ops "after"))
    (is-after (j/query-for-row ops "after" [30]))
    (is-after (j/query-for-val ops "after"))
    (is-after (j/query-for-val ops "after" [30]))
    (is-after (j/update ops "after"))
    (is-after (j/update ops "after" [30]))
    (is-after (j/genkey ops "after"))
    (is-after (j/genkey ops "after" [30]))
    (is-after (j/genkey-via ops identity "after"))
    (is-after (j/genkey-via ops identity "after" [30]))))
