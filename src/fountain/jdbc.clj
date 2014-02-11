(ns fountain.jdbc
  (:require [fountain.jdbc.util :as u])
  (:import (org.springframework.jdbc.support KeyHolder)
           (org.springframework.transaction  TransactionStatus)))


;; ===== JDBC Operations =====


(defprotocol Ops
  (batch-update   [this sql-coll]
                  [this sql args-coll]
                  "Batch updates.")
  (execute        [this sql]
                  "Execute arbitrary statement, typically DDL.")
  (lazy-query     [this f colnames? sql]
                  [this f colnames? sql args]
                  "Lazily fetch query result, handled by f(2) accepting colnames and lazy-datarows.")
  (query-datalist [this sql]
                  [this sql args]
                  "Fetch only data rows without column names -- [[] datarows].")
  (query-datarow  [this sql]
                  [this sql args]
                  "Fetch only single row of data without column names -- [[] colvalues].")
  (query-for-list [this sql]
                  [this sql args]
                  "Fetch column names and data rows -- [colnames datarows].")
  (query-for-row  [this sql]
                  [this sql args]
                  "Fetch column names and single row of data -- [colnames colvalues].")
  (query-for-val  [this sql]
                  [this sql args]
                  "Fetch column value from single column and row.")
  (update         [this sql]
                  [this sql args]
                  "Execute update (insert/update/delete) statement, returning effected rowcount.")
  (genkey         [this sql]
                  [this sql args]
                  "Execute typically insert/update statement, returning single generated key.")
  (genkey-via     [this f sql]
                  [this f sql args]
                  "Execute typically insert/update statement, using f(2) to return generated keys.
  Fn f is called with args fn(1) (to convert DB entity name to Clojure equiv)
  and org.springframework.jdbc.support.KeyHolder instance."))


;; ----- Query result handling -----


(defn result-to-maplist
  ([db-to-clj result]
    (let [[colnames rows] result
          cols (map db-to-clj colnames)]
      (map #(zipmap cols %) rows)))
  ([result]
    (result-to-maplist identity result)))


;; ----- KeyHolder fns (can be used with `genkey-via`) -----


(defn get-key
  ([^KeyHolder holder]
    (.getKey holder))
  ([_ ^KeyHolder holder]
    (.getKey holder)))


(defn get-keys
  [db-to-clj ^KeyHolder holder]
  (u/map-keys db-to-clj (.getKeys holder)))


(defn get-keylist
  [db-to-clj ^KeyHolder holder]
  (->> (.getKeyList holder)
    (map #(u/map-keys db-to-clj %))
    doall))


;; ===== Transactions  =====


(defprotocol Txn
  (commit [this]   "Commit current transaction")
  (rollback [this] "Rollback current transaction"))


(defprotocol TxnAccess
  (get-txn [this]   "Get Txn instance in order to manually execute commit or rollback operations")
  (in-txn  [this f] "Execute f(0) in a transaction and commit/rollback based on whether exceptions are thrown"))


(defmacro do-in-txn
  [txn-access & body]
  `(in-txn ~txn-access (^:once fn* [] ~@body)))
