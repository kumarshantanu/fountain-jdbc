(ns org.bituf.fountain.test-jdbc
  (:import
    (java.sql Connection Statement))
  (:require
    [clojure.java.io         :as io]
    [clojure.pprint          :as pp]
    [org.bituf.fountain.jdbc :as jd]
    [org.bituf.clj-dbcp      :as cp]
    [org.bituf.clj-miscutil  :as mu]
    [org.bituf.clj-dbspec    :as sp])
  (:use clojure.test))


(def ds (cp/h2-memory-datasource))


(def dbspec (sp/make-dbspec ds))


(defmacro with-stmt
  [st & body]
  `((sp/wrap-connection
      (fn []
        (with-open [~st (.createStatement ^Connection (:connection sp/*dbspec*))]
          ~@body)))))


(defn setup
  []
  (with-stmt ^Statement st
    (mu/maybe (.execute st "DROP TABLE sample"))
    (.execute st "CREATE TABLE sample
                   (sample_id INT         NOT NULL PRIMARY KEY AUTO_INCREMENT,
                    name      VARCHAR(30) NOT NULL,
                    age       INT)")
    (.execute st "INSERT INTO sample (name, age)
                  VALUES ('Harry', 30)")))


(defn setup2
  []
  (with-stmt ^Statement st
    (mu/maybe (.execute st "DROP TABLE sample2"))
    (.execute st "CREATE TABLE sample2
                   (sample_id INT         NOT NULL PRIMARY KEY AUTO_INCREMENT,
                    extra_id  INT         NOT NULL AUTO_INCREMENT,
                    name      VARCHAR(30) NOT NULL,
                    age       INT)")
    (.execute st "INSERT INTO sample2 (name, age)
                  VALUES ('Harry', 30)")))


(defn fail
  ([msg] (is false msg))
  ([] (is false)))


(defn query-for-int-test
  []
  (setup)
  (is (= 30 (jd/query-for-int "SELECT age FROM sample WHERE age = 30")))
  (is (= 30 (jd/query-for-int "SELECT age FROM sample WHERE age = ?" [30])))
  (is (= 30 (jd/query-for-int "SELECT age FROM sample WHERE age = :age"
              {:age 30}))))

(deftest test-query-for-int
  (testing "test-query-for-int"
    ((sp/wrap-dbspec dbspec
       (jd/wrap-sjt
         query-for-int-test)))))


(defn query-for-long-test
  []
  (setup)
  (is (= 30 (jd/query-for-long "SELECT age FROM sample WHERE age = 30")))
  (is (= 30 (jd/query-for-long "SELECT age FROM sample WHERE age = ?" [30])))
  (is (= 30 (jd/query-for-long "SELECT age FROM sample WHERE age = :age"
              {:age 30}))))


(deftest test-query-for-long
  (testing "test-query-for-long"
    ((sp/wrap-dbspec dbspec
       (jd/wrap-sjt query-for-long-test)))))


(defn query-for-map-test
  []
  (setup)
  (is (= {:age 30}
        (jd/query-for-map "SELECT age FROM sample WHERE age = 30")))
  (is (= {:age 30}
        (jd/query-for-map "SELECT age FROM sample WHERE age = ?" [30])))
  (is (= {:age 30}
        (jd/query-for-map "SELECT age FROM sample WHERE age = :age"
          {:age 30}))))

(deftest test-query-for-map
  (testing "test-query-for-map"
    ((sp/wrap-dbspec dbspec
       (jd/wrap-sjt query-for-map-test)))))


(defn query-for-list-test
  []
  (setup)
  (is (= [{:age 30}]
        (jd/query-for-list "SELECT age FROM sample WHERE age = 30")))
  (is (= [{:age 30}]
        (jd/query-for-list "SELECT age FROM sample WHERE age = ?" [30])))
  (is (= [{:age 30}]
        (jd/query-for-list "SELECT age FROM sample WHERE age = :age"
          {:age 30}))))


(deftest test-query-for-list
  (testing "test-query-for-list"
    ((sp/wrap-dbspec dbspec
       (jd/wrap-sjt query-for-list-test)))))


(defn update-test
  []
  (setup)
  (is (= 1 (jd/update "UPDATE sample SET age=40 WHERE age=30")))
  (is (= 1 (jd/update "UPDATE sample SET age=50 WHERE age=?" [40])))
  (is (= 1 (jd/update "UPDATE sample SET age=60 WHERE age=:age"
             {:age 50}))))


(deftest test-update
  (testing "test-update"
    ((sp/wrap-dbspec dbspec
       (jd/wrap-sjt update-test)))))


(defn batch-update-test
  []
  (setup)
  (is (= [1 1] (jd/batch-update
                 "INSERT INTO sample (name, age) VALUES (:name, :age)"
                 [{:name "Hello" :age 20}
                  {:name "World" :age 40}]))))


(deftest test-batch-update
  (testing "test-batch-update"
    ((sp/wrap-dbspec dbspec
       (jd/wrap-sjt batch-update-test)))))


(defn insert-test
  []
  (setup)
  (let [sji (jd/make-sji :sample)]
    (is (= 1 (jd/insert sji {:name "Hello" :age 20})))))

(deftest test-insert
  (testing "test-insert"
    ((sp/wrap-dbspec dbspec
       (jd/wrap-sjt insert-test)))))


(defn insert-give-id-test
  []
  (setup)
  (let [sji (mu/! (jd/make-sji "sample" :gencols :sample-id))]
    (is (= 2 (jd/insert-give-id sji {:name "Hello" :age 20})))))


(deftest test-insert-give-id
  (testing "test-insert-give-id"
    ((sp/wrap-dbspec dbspec
       (jd/wrap-sjt insert-give-id-test)))))


(defn insert-give-idmap-test
  []
  (setup)
  (let [sji (jd/make-sji :sample :gencols :sample-id)]
    (is (= 2 (first (vals (jd/insert-give-idmap sji
                            {:name "Hello" :age 20}))))))
  (setup2)
  (let [sji (jd/make-sji :sample2 :gencols [:sample-id :extra-id])]
    (is (= 2 (first (vals (jd/insert-give-idmap sji
                            {:name "Hello" :age 20})))))))


(deftest test-insert-give-idmap
  (testing "test-insert-give-idmap"
    ((sp/wrap-dbspec dbspec
       (jd/wrap-sjt insert-give-idmap-test)))))


(defn insert-batch-test
  []
  (setup)
  (let [sji (jd/make-sji :sample :gencols :sample-id)]
    (is (= [1 1]
          (jd/insert-batch sji [{:name "Hello" :age 20}
                                {:name "World" :age 30}])))))


(deftest test-insert-batch
  (testing "test-insert-batch"
    ((sp/wrap-dbspec dbspec
       (jd/wrap-sjt insert-batch-test)))))


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
