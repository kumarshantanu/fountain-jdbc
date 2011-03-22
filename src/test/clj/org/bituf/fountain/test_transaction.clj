(ns org.bituf.fountain.test-transaction
  (:import
    (java.lang.reflect               UndeclaredThrowableException)
    (java.sql                        Connection Statement)
    (org.springframework.transaction TransactionDefinition)
    (org.bituf.clj_dbspec            WriteNotAllowedException))
  (:require
    [clojure.java.io         :as io]
    [clojure.pprint          :as pp]
    [org.bituf.fountain.jdbc :as jd]
    [org.bituf.fountain.transaction :as tx]
    [org.bituf.clj-dbcp      :as cp]
    [org.bituf.clj-miscutil  :as mu]
    [org.bituf.clj-dbspec    :as sp]
    [org.bituf.fountain.test-util :as tu])
  (:use clojure.test))


(def ds (cp/h2-memory-datasource))


(def dbspec (-> (sp/make-dbspec ds)
              jd/assoc-sjt
              tx/assoc-txnt))


(defn custom-dbspec
  "Create custom DB-Spec using a transaction definition"
  [txndef]
  (-> (sp/make-dbspec ds)
    jd/assoc-sjt
    (tx/assoc-txnt #(tx/make-txntspec :txndef txndef))))


(defmacro do-in-txn
  [txntspec & body] {:pre [`(map? ~txntspec)]}
  `(let [g# (tx/wrap-txncallback ~txntspec
              (fn [] ~@body))]
     (g#)))


(defn commit-test
  []
  (tu/sample-setup)
  (is (= 1 (tu/row-count :sample)))
  (let [sji (jd/make-sji :sample)]
    (do-in-txn dbspec
      (jd/insert sji {:name "Hello" :age 20})
      (jd/insert sji {:name "Hola"  :age 40})))
  (is (= 3 (tu/row-count :sample))))


(deftest test-commit
  (testing "Basic transaction commit"
    (sp/with-dbspec dbspec
      (commit-test))))


(defn rollback-test
  [^Class ec ^Throwable ex]
  (tu/sample-setup)
  (is (= 1 (tu/row-count :sample)))
  (tu/is-thrown? ec
    (let [sji (jd/make-sji :sample)]
      (do-in-txn dbspec
        (jd/insert sji {:name "Rollback 1" :age 20})
        (throw ex)
        (jd/insert sji {:name "Rollback 2" :age 40}))))
  (is (= 1 (tu/row-count :sample))))


(deftest test-rollback
  (testing "Basic transaction rollback with unchecked exception"
    (sp/with-dbspec dbspec
      (rollback-test NullPointerException (NullPointerException.))))
  (testing "Basic transaction rollback with checked exception"
    (sp/with-dbspec dbspec
      (rollback-test UndeclaredThrowableException (java.io.IOException.))))
  (testing "Basic transaction rollback with Error"
    (sp/with-dbspec dbspec
      (rollback-test AssertionError (AssertionError.)))))


(defn txndef?
  [td]
  (instance? TransactionDefinition td))


(deftest test-custom-txndef
  (testing "Default transaction definition"
    (is (txndef? (tx/make-txndef)) "Default txn definition"))
  (testing "Custom transaction definition (positive tests)"
    (is (txndef? (tx/make-txndef :isolation   :repeatable-read)) "With isolation level")
    (is (txndef? (tx/make-txndef :propagation :requires-new))    "With propagation behavior")
    (is (txndef? (tx/make-txndef :read-only   true))             "Read-only txn def")
    (is (txndef? (tx/make-txndef :rollback-on (fn [ex] true)))   "With predicate based rollback")
    (is (txndef? (tx/make-txndef :rollback-for    ["Exception" NullPointerException])) "With rule based rollback")
    (is (txndef? (tx/make-txndef :no-rollback-for [Exception "NullPointerException"])) "With rule based rollback")
    (is (txndef? (tx/make-txndef :timeout-sec 4))                "With timeout period")
    (is (txndef? (tx/make-txndef :txn-name    "Some name"))      "With timeout period"))
  (testing "Transaction definition (negative tests)"
    (is (thrown? Exception (tx/make-txndef :bad-arg         :bad-value)) "Bad argument")
    (is (thrown? Exception (tx/make-txndef :isolation       :bad-value)) "With isolation level")
    (is (thrown? Exception (tx/make-txndef :propagation     :bad-value)) "With propagation behavior")
    (is (thrown? Exception (tx/make-txndef :read-only       :bad-value)) "Read-only txn def")
    (is (thrown? Exception (tx/make-txndef :rollback-on     :bad-value)) "With predicate based rollback")
    (is (thrown? Exception (tx/make-txndef :rollback-for    :bad-value)) "With rule based rollback")
    (is (thrown? Exception (tx/make-txndef :no-rollback-for :bad-value)) "With rule based rollback")
    (is (thrown? Exception (tx/make-txndef :timeout-sec     :bad-value)) "With timeout period")
    (is (thrown? Exception (tx/make-txndef :txn-name        :bad-value)) "With timeout period")))


(defn regular
  [spec sji]
  (tu/sample-setup)
  (is (= 1 (tu/sample-count)))
  (do-in-txn spec
    (jd/insert sji {:name "Rollback 1" :age 20})
    (jd/insert sji {:name "Rollback 2" :age 40})))


(defn throwex
  [spec sji ex]
  (tu/sample-setup)
  (is (= 1 (tu/sample-count)))
  (do-in-txn spec
    (jd/insert sji {:name "Rollback 1" :age 20})
    (throw ex)
    (jd/insert sji {:name "Rollback 2" :age 40})))


(deftest test-predicate-based-rollback
  (testing "Predicate based rollback"
    (let [ct (tx/make-txndef :rollback-on
               (fn [^Throwable t]
                 (not (instance? UnsupportedOperationException t))))
          cd (custom-dbspec ct)]
      (sp/with-dbspec cd
        (is (not (.rollbackOn (.getTransactionAttribute (tx/get-txnt))
                   (UnsupportedOperationException.))))
        (let [sji (jd/make-sji :sample)]
          (regular cd sji)
          (is (= 3 (tu/sample-count)))
          (is (thrown? UnsupportedOperationException
                (throwex cd sji (UnsupportedOperationException.))))
          (is (= 2 (tu/sample-count))))))))


(deftest test-rule-based-rollback
  (testing "Rule based rollback"
    (let [ct (tx/make-txndef
               :rollback-for    [NullPointerException]
               :no-rollback-for [UnsupportedOperationException])
          cd (custom-dbspec ct)]
      (sp/with-dbspec cd
        ;; no-rollback-for
        (is (not (.rollbackOn (.getTransactionAttribute (tx/get-txnt))
                   (UnsupportedOperationException.))))
        (let [sji (jd/make-sji :sample)]
          (regular cd sji)
          (is (= 3 (tu/sample-count)))
          (is (thrown? UnsupportedOperationException
                (throwex cd sji (UnsupportedOperationException.))))
          (is (= 2 (tu/sample-count))))
        ;; rollback-for
        (is (.rollbackOn (.getTransactionAttribute (tx/get-txnt))
              (NullPointerException.)))
        (let [sji (jd/make-sji :sample)]
          (regular cd sji)
          (is (= 3 (tu/sample-count)))
          (is (thrown? NullPointerException
                (throwex cd sji (NullPointerException.))))
          (is (= 1 (tu/sample-count))))))))


(deftest test-readonly-txn
  (testing "Read-only transaction"
    (sp/with-dbspec (custom-dbspec (tx/make-txndef :read-only true))
      (is (thrown? WriteNotAllowedException
            (commit-test))))))


;; test-txn-timeout - not well supported by underlying transaction manager


(defn test-ns-hook []
  (test-commit)
  (test-rollback)
  (test-custom-txndef)
  (test-predicate-based-rollback)
  (test-rule-based-rollback)
  (test-readonly-txn))
