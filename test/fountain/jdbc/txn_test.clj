(ns fountain.jdbc.txn-test
  (:require [fountain.jdbc :as j]
            [fountain.jdbc.factory   :as f]
            [fountain.jdbc.impl-txn  :as t]
            [fountain.jdbc.test-util :as tu]
            [clj-dbcp.core           :as cp]
            [clojure.test :refer :all])
  (:import
    (java.lang.reflect UndeclaredThrowableException)
    (fountain.jdbc CustomTransactionTemplate TxnAccess)))


(def dbspec (f/assoc-txn-manager {:datasource (cp/make-datasource :h2 {:target :memory :database "emp"
                                                                       :user "sa" :password ""})}))


(def txn-access (f/make-txn-access dbspec))


(defn commit-test
  [dbspec]
  (tu/with-sample dbspec ops npops
    (is (= 2 (tu/sample-count ops)))
    (j/do-in-txn txn-access
                 (j/batch-update
                   ops "INSERT INTO sample (name, age, gender, is_emp) VALUES (?, ?, ?, ?)"
                   [["David" 50 "M" false]
                    ["Helga" 60 "F" false]]))
    (is (= 4 (tu/sample-count ops)))))


(deftest test-commit
  (testing "Basic transaction commit"
           (commit-test dbspec)))


(defn rollback-test
  [^Class ec ^Throwable ex]
  (tu/with-sample dbspec ops npops
    (is (= 2 (tu/sample-count ops)))
    (tu/is-thrown? ec
                   (j/do-in-txn txn-access
                                (j/update ops "INSERT INTO sample (name, age, gender, is_emp) VALUES (?, ?, ?, ?)"
                                          ["David" 50 "M" false])
                                (throw ex)
                                (j/update ops "INSERT INTO sample (name, age, gender, is_emp) VALUES (?, ?, ?, ?)"
                                          ["Helga" 60 "F" false])))
    (is (= 2 (tu/sample-count ops)))))


(deftest test-rollback
  (testing "Basic transaction rollback with unchecked exception"
           (rollback-test NullPointerException (NullPointerException.)))
  (testing "Basic transaction rollback with checked exception"
           (rollback-test UndeclaredThrowableException (java.io.IOException.)))
  (testing "Basic transaction rollback with Error"
           (rollback-test AssertionError (AssertionError.))))


(defn txndef?
  [td]
  (instance? TxnAccess td))


(deftest test-custom-txndef
  (testing "Default transaction definition"
    (is (txndef? (f/make-txn-access dbspec)) "Default txn definition"))
  (testing "Custom transaction definition (positive tests)"
    (is (txndef? (f/make-txn-access (assoc dbspec :isolation   :repeatable-read))) "With isolation level")
    (is (txndef? (f/make-txn-access (assoc dbspec :propagation :requires-new)))    "With propagation behavior")
    (is (txndef? (f/make-txn-access (assoc dbspec :read-only?  true)))             "Read-only txn def")
    (is (txndef? (f/make-txn-access (assoc dbspec :rollback-on (fn [ex] true))))   "With predicate based rollback")
    (is (txndef? (f/make-txn-access (assoc dbspec :rollback-for    ["Exception" NullPointerException]))) "With rule based rollback")
    (is (txndef? (f/make-txn-access (assoc dbspec :no-rollback-for [Exception "NullPointerException"]))) "With rule based rollback")
    (is (txndef? (f/make-txn-access (assoc dbspec :timeout-sec 4)))                "With timeout period")
    (is (txndef? (f/make-txn-access (assoc dbspec :txn-name    "Some name")))      "With timeout period"))
  (testing "Transaction definition (negative tests)"
   (is (thrown? Exception (f/make-txn-access :bad-arg         :bad-value)) "Bad argument")
   (is (thrown? Exception (f/make-txn-access (assoc dbspec :isolation       :bad-value))) "With isolation level")
   (is (thrown? Exception (f/make-txn-access (assoc dbspec :propagation     :bad-value))) "With propagation behavior")
   (is (thrown? Exception (f/make-txn-access (assoc dbspec :rollback-on     :bad-value))) "With predicate based rollback")
   (is (thrown? Exception (f/make-txn-access (assoc dbspec :rollback-for    :bad-value))) "With rule based rollback")
   (is (thrown? Exception (f/make-txn-access (assoc dbspec :no-rollback-for :bad-value))) "With rule based rollback")
   (is (thrown? Exception (f/make-txn-access (assoc dbspec :timeout-sec     :bad-value))) "With timeout period")
   (is (thrown? Exception (f/make-txn-access (assoc dbspec :txn-name        :bad-value))) "With timeout period")))


(defn regular
  [spec txn-access ops]
  (tu/sample-setup spec)
  (is (= 2 (tu/sample-count ops)))
  (j/do-in-txn txn-access
    (j/batch-update
      ops "INSERT INTO sample (name, age, gender, is_emp) VALUES (?, ?, ?, ?)"
      [["David" 50 "M" false]
       ["Helga" 60 "F" false]])))


(defn throwex
  [spec txn-access ops ex]
  (tu/sample-setup spec)
  (is (= 2 (tu/sample-count ops)))
  (j/do-in-txn txn-access
    (j/update ops "INSERT INTO sample (name, age, gender, is_emp) VALUES (?, ?, ?, ?)" ["David" 50 "M" false])
    (throw ex)
    (j/update ops "INSERT INTO sample (name, age, gender, is_emp) VALUES (?, ?, ?, ?)" ["Helga" 60 "F" false])))


(deftest test-predicate-based-rollback
  (testing "Predicate based rollback"
           (let [spec (assoc dbspec :rollback-on (fn [^Throwable t]
                                                   (not (instance? UnsupportedOperationException t))))
                 txna (f/make-txn-access spec)
                 cops (f/make-ops-impl spec)]
             (is (not (.rollbackOn (.getTransactionAttribute ^CustomTransactionTemplate (:txn-template txna))
                        (UnsupportedOperationException.))))
             (regular spec txna cops)
             (is (= 4 (tu/sample-count cops)))
             (is (thrown? UnsupportedOperationException
                          (throwex spec txna cops (UnsupportedOperationException.))))
             (is (= 3 (tu/sample-count cops))))))


(deftest test-rule-based-rollback
  (testing "Rule based rollback"
    (let [spec (assoc dbspec
                      :rollback-for    [NullPointerException]
                      :no-rollback-for [UnsupportedOperationException])
          txna (f/make-txn-access spec)
          cops (f/make-ops-impl spec)]
      ;; no-rollback-for
      (is (not (.rollbackOn (.getTransactionAttribute ^CustomTransactionTemplate (:txn-template txna))
                 (UnsupportedOperationException.))))
      (regular spec txna cops)
      (is (= 4 (tu/sample-count cops)))
      (is (thrown? UnsupportedOperationException
                   (throwex spec txna cops (UnsupportedOperationException.))))
      (is (= 3 (tu/sample-count cops)))
      ;; rollback-for
      (is (.rollbackOn (.getTransactionAttribute ^CustomTransactionTemplate (:txn-template txna))
            (NullPointerException.)))
      (regular spec txna cops)
      (is (= 4 (tu/sample-count cops)))
      (is (thrown? NullPointerException
                   (throwex spec txna cops (NullPointerException.))))
      (is (= 2 (tu/sample-count cops))))))
