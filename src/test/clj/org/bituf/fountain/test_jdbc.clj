(ns org.bituf.fountain.test-jdbc
  (:import
    (java.sql Connection Statement))
  (:require
    [clojure.java.io         :as io]
    [clojure.pprint          :as pp]
    [org.bituf.fountain.jdbc :as jd]
    [org.bituf.clj-dbcp      :as cp]
    [org.bituf.clj-miscutil  :as mu]
    [org.bituf.clj-dbspec    :as sp]
    [org.bituf.fountain.test-util :as tu])
  (:use clojure.test))


(def ds (cp/h2-memory-datasource))


(def dbspec (sp/make-dbspec ds))


(defn do-with-dbspec
  [f] {:post [(mu/not-fn? %)]
       :pre  [(fn? f)]}
  (let [g (sp/wrap-dbspec dbspec f)]
    (g)))


(defn do-with-sjt
  "Execute f in the context of datasource and SimpleJdbcTemplate"
  [f] {:post [(mu/not-fn? %)]
       :pre  [(fn? f)]}
  (let [g (sp/wrap-dbspec dbspec
            (jd/wrap-sjt f))]
    (g)))


(defn query-for-int-test
  []
  (tu/sample-setup)
  (is (= 30 (jd/query-for-int "SELECT age FROM sample WHERE age = 30")))
  (is (= 30 (jd/query-for-int "SELECT age FROM sample WHERE age = ?" [30])))
  (is (= 30 (jd/query-for-int "SELECT age FROM sample WHERE age = :age"
              {:age 30}))))


(deftest test-query-for-int
  (testing "test-query-for-int"
    (do-with-sjt
      query-for-int-test)))


(defn query-for-long-test
  []
  (tu/sample-setup)
  (is (= 30 (jd/query-for-long "SELECT age FROM sample WHERE age = 30")))
  (is (= 30 (jd/query-for-long "SELECT age FROM sample WHERE age = ?" [30])))
  (is (= 30 (jd/query-for-long "SELECT age FROM sample WHERE age = :age"
              {:age 30}))))


(deftest test-query-for-long
  (testing "test-query-for-long"
    (do-with-sjt
      query-for-long-test)))


(defn query-for-map-test
  []
  (tu/sample-setup)
  (is (= {:age 30}
        (jd/query-for-map "SELECT age FROM sample WHERE age = 30")))
  (is (= {:age 30}
        (jd/query-for-map "SELECT age FROM sample WHERE age = ?" [30])))
  (is (= {:age 30}
        (jd/query-for-map "SELECT age FROM sample WHERE age = :age"
          {:age 30}))))

(deftest test-query-for-map
  (testing "test-query-for-map"
    (do-with-sjt
      query-for-map-test)))


(defn query-for-list-test
  []
  (tu/sample-setup)
  (is (= [{:age 30}]
        (jd/query-for-list "SELECT age FROM sample WHERE age = 30")))
  (is (= [{:age 30}]
        (jd/query-for-list "SELECT age FROM sample WHERE age = ?" [30])))
  (is (= [{:age 30}]
        (jd/query-for-list "SELECT age FROM sample WHERE age = :age"
          {:age 30}))))


(deftest test-query-for-list
  (testing "test-query-for-list"
    (do-with-sjt
      query-for-list-test)))


(defn update-test
  []
  (tu/sample-setup)
  (is (= 1 (jd/update "UPDATE sample SET age=40 WHERE age=30")))
  (is (= 1 (jd/update "UPDATE sample SET age=50 WHERE age=?" [40])))
  (is (= 1 (jd/update "UPDATE sample SET age=60 WHERE age=:age"
             {:age 50}))))


(deftest test-update
  (testing "test-update"
    (do-with-sjt
      update-test)))


(defn batch-update-test
  []
  (tu/sample-setup)
  (is (= [1 1] (jd/batch-update
                 "INSERT INTO sample (name, age) VALUES (:name, :age)"
                 [{:name "Hello" :age 20}
                  {:name "World" :age 40}]))))


(deftest test-batch-update
  (testing "test-batch-update"
    (do-with-sjt
      batch-update-test)))


(defn insert-test
  []
  (tu/sample-setup)
  (let [sji (jd/make-sji :sample)]
    (is (= 1 (jd/insert sji {:name "Hello" :age 20})))))

(deftest test-insert
  (testing "test-insert"
    (do-with-dbspec
      insert-test)))


(defn insert-give-id-test
  []
  (tu/sample-setup)
  (let [sji (mu/! (jd/make-sji "sample" :gencols :sample-id))]
    (is (= 2 (jd/insert-give-id sji {:name "Hello" :age 20})))))


(deftest test-insert-give-id
  (testing "test-insert-give-id"
    (do-with-dbspec
      insert-give-id-test)))


(defn insert-give-idmap-test
  []
  (tu/sample-setup)
  (let [sji (jd/make-sji :sample :gencols :sample-id)]
    (is (= 2 (first (vals (jd/insert-give-idmap sji
                            {:name "Hello" :age 20}))))))
  (tu/sample2-setup)
  (let [sji (jd/make-sji :sample2 :gencols [:sample-id :extra-id])]
    (is (= 2 (first (vals (jd/insert-give-idmap sji
                            {:name "Hello" :age 20})))))))


(deftest test-insert-give-idmap
  (testing "test-insert-give-idmap"
    (do-with-dbspec
      insert-give-idmap-test)))


(defn insert-batch-test
  []
  (tu/sample-setup)
  (let [sji (jd/make-sji :sample :gencols :sample-id)]
    (is (= [1 1]
          (jd/insert-batch sji [{:name "Hello" :age 20}
                                {:name "World" :age 30}])))))


(deftest test-insert-batch
  (testing "test-insert-batch"
    (do-with-dbspec
      insert-batch-test)))


(defn test-ns-hook []
  (test-query-for-int)
  (test-query-for-long)
  (test-query-for-map)
  (test-query-for-list)
  (test-update)
  (test-batch-update)
  (test-insert)
  (test-insert-give-id)
  (test-insert-give-idmap)
  (test-insert-batch))
