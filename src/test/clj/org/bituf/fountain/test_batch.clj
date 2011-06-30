(ns org.bituf.fountain.test-batch
  (:require
    [clojure.java.io          :as io]
    [clojure.pprint           :as pp]
    ;[org.bituf.fountain.jdbc :as jd]
    [org.bituf.fountain.batch :as ba]
    [org.bituf.clj-dbcp       :as cp]
    [org.bituf.clj-miscutil   :as mu]
    [org.bituf.clj-dbspec     :as sp]
    [org.bituf.fountain.test-util :as tu])
  (:import
    (org.springframework.batch.item.database JdbcCursorItemReader
                                             JdbcPagingItemReader))
  (:use clojure.test))


(def ds (cp/h2-memory-datasource))


(def dbspec (sp/make-dbspec ds))


(defn do-with-dbspec
  [f] {:post [(mu/not-fn? %)]
       :pre  [(fn? f)]}
  (let [g (sp/wrap-dbspec dbspec f)]
    (g)))


;(defn do-with-sjt
;  "Execute f in the context of datasource and SimpleJdbcTemplate"
;  [f] {:post [(mu/not-fn? %)]
;       :pre  [(fn? f)]}
;  (let [g (sp/wrap-dbspec dbspec
;            (jd/wrap-sjt f))]
;    (g)))


(defn jdbc-cursor-reader-default-config-test
  [sql & args]
  (tu/sample-ba1-setup)
  (is (= (into []
               (ba/row-seq (ba/jdbc-cursor-reader (into [sql] args))))
         [{:age 30}])))


(deftest test-cursor-reader-works-with-default-config
  (testing "With no params"
           (do-with-dbspec
             (partial jdbc-cursor-reader-default-config-test
                      "SELECT age FROM sample WHERE age = 30")))
  (testing "With positional params"
           (do-with-dbspec
             (partial jdbc-cursor-reader-default-config-test
                      "SELECT age FROM sample WHERE age = ?" 30)))
  (testing "With named params"
           (do-with-dbspec
             (partial jdbc-cursor-reader-default-config-test
                      "SELECT age FROM sample WHERE age = :age" {:age 30}))))


(defn jdbc-cursor-reader-custom-config-test
  [sql & args]
  (tu/sample-ba1-setup)
  (with-open [r (JdbcCursorItemReader.)]
    (is (= (into []
                 (ba/row-seq (ba/jdbc-cursor-reader (into [sql] args)
                                                    :jci-reader r
                                                    :fetch-size 10)))
           [{:age 30}]))))


(deftest test-cursor-reader-works-with-custom-config
  (testing "Custom config"
           (do-with-dbspec
             (partial jdbc-cursor-reader-custom-config-test
                      "SELECT age FROM sample WHERE age = :age" {:age 30}))))


(defn jdbc-paging-reader-helper
  [criteria & args]
  (tu/sample-ba1-setup)
  (into []
        (ba/row-seq (apply ba/jdbc-paging-reader criteria args))))


(deftest test-paging-reader-works-with-default-config
  (testing "With no params"
           (is (= (do-with-dbspec
                    (partial jdbc-paging-reader-helper
                             (ba/make-criteia :select   "SELECT age"     :from     "FROM sample"
                                              :where    "WHERE age = 30" :sort-key "age"
                                              ;:sort-asc ;:args
                                              )))
                  [{:age 30}])))
  (testing "With positional params"
           (is (= (do-with-dbspec
                    (partial jdbc-paging-reader-helper
                             (ba/make-criteia :select   "SELECT age"    :from     "FROM sample"
                                              :where    "WHERE age = ?" :sort-key "age"
                                              ;:sort-asc
                                              :args [30])))
                  [{:age 30}])))
  (testing "With named params"
           (is (= (do-with-dbspec
                    (partial jdbc-paging-reader-helper
                             (ba/make-criteia :select   "SELECT age"       :from     "FROM sample"
                                              :where    "WHERE age = :age" :sort-key "age"
                                              ;:sort-asc
                                              :args {:age 30})))
                  [{:age 30}])))
  (testing "With sort-asc false (i.e. Descending order)"
           (is (= (do-with-dbspec
                    (partial jdbc-paging-reader-helper
                             (ba/make-criteia :select   "SELECT age" :from     "FROM sample"
                                              :sort-key "age"        :sort-asc false)))
                  [{:age 40} {:age 30}]))))


(deftest test-paging-reader-works-with-custom-config
  (testing "With custom config"
           (is (= (do-with-dbspec
                    (partial jdbc-paging-reader-helper
                             (ba/make-criteia :select   "SELECT age" :from     "FROM sample"
                                              :sort-key "age"        :sort-asc false)
                             :fetch-size 100
                             :save-state false
                             :start-pos  1
                             :max-count  100))
                  [{:age 40} {:age 30}]))))


(deftest test-paging-reader-can-paginate
  (testing "With pagination"
           (do-with-dbspec
             #(let [_ (tu/sample-ba1-setup)
                    jpi-reader (JdbcPagingItemReader.)
                    reader-fn  (ba/jdbc-paging-reader
                                 (ba/make-criteia :select   "SELECT age" :from     "FROM sample"
                                                  :sort-key "age"        :sort-asc false)
                                 :jpi-reader jpi-reader)]
                (ba/set-page! jpi-reader 1 1)
                (is (= (into [] (ba/row-seq reader-fn)) [{:age 40}]))
                (ba/set-page! jpi-reader 1 2)
                (is (= (into [] (ba/row-seq reader-fn)) [{:age 30}]))))))


(defn test-ns-hook []
  (test-cursor-reader-works-with-default-config)
  (test-cursor-reader-works-with-custom-config)
  (test-paging-reader-works-with-default-config)
  (test-paging-reader-works-with-custom-config)
  (test-paging-reader-can-paginate))