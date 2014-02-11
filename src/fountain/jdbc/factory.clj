(ns fountain.jdbc.factory
  (:require [fountain.jdbc.impl-ops :as i]
            [fountain.jdbc.impl-txn :as t]
            [fountain.jdbc.util :as u])
  (:import (fountain.jdbc CustomTransactionTemplate Ops TxnAccess)
           (javax.sql DataSource)
           (org.springframework.jdbc.datasource DataSourceTransactionManager)
           (org.springframework.transaction PlatformTransactionManager)
           (org.springframework.transaction.interceptor DefaultTransactionAttribute)))


(defn ^Ops make-ops-impl
  "Given map `dbspec` containing required key :datasource with javax.sql.DataSource value, and
  optional kwargs below, return an appropriate Ops instance.
     Keyword       Value      Description
     -------       -----      -----------
    :fetch-size    integer    When +ve number, sets limit on JDBC-driver fetch size
    :query-timeout integer    When +ve number, sets timeout on JDBC-driver query timeout
    :max-rows      integer    When +ve number, sets limit on JDBC-driver max result rows
    :clj-to-db     fn(1)      Converts Clojure entity names to DB equivalent (default supplied)
    :db-to-clj     fn(1)      Converts DB entity names to Clojure equivalent (default supplied)
    :before-fn     fn(1)      When defined, called with SQL as argument (possibly to log or print)
    :after-fn      fn(2)      When defined, called with SQL and millis-elapsed as arguments
    :read-only?    boolean    When true, throws UnsupportedOperationException on non-readonly calls
    :named-params? boolean    This is for SQL prepared-statement arguments only:
                              Default false, args expected to be list, values are set by position
                              When true, args expected to be a map of param name/keyword to value"
  [dbspec] {:pre [(map? dbspec)
                  (instance? DataSource (:datasource dbspec))]}
  (let [{:keys [datasource named-params?  ; template construction
                clj-to-db  db-to-clj      ; entity name conversion
                fetch-size query-timeout max-rows  ; template settings
                read-only? before-fn     after-fn] ; decorators
         :or {clj-to-db u/default-clj-to-db
              db-to-clj u/default-db-to-clj}} dbspec
        dbspec (assoc dbspec :clj-to-db clj-to-db :db-to-clj db-to-clj)
        ^Ops base-ops (if named-params?
                        (i/make-npops-impl datasource dbspec)
                        (i/make-ops-impl datasource dbspec))
        wrap-if (fn ([test f val] (if test (f val) val))
                  ([test f arg val] (if test (f val arg) val)))]
    (->> base-ops
      (wrap-if before-fn  i/wrap-ops-before before-fn)
      (wrap-if after-fn   i/wrap-ops-after  after-fn)
      (wrap-if read-only? i/wrap-ops-read-only))))


(defn ^PlatformTransactionManager assoc-txn-manager
  "Given map `platform-txnspec` containing required key :datasource with javax.sql.DataSource value, return an
  appropriate org.springframework.transaction.PlatformTransactionManager instance that should be reused for all
  transactions wit respect to the datasource."
  [platform-txnspec] {:pre [(map? platform-txnspec)
                            (instance? DataSource (:datasource platform-txnspec))]}
  (let [{:keys [datasource]} platform-txnspec]
    (assoc platform-txnspec
           :platform-txn-manager (DataSourceTransactionManager. datasource))))


(defn ^TxnAccess make-txn-access
  "Given map `txnspec` containing required key :platform-txn-manager with value of type
  org.springframework.transaction.PlatformTransactionManager and optional kwargs below, return a TxnAccess instance.
     Keyword          Description
     -------          -----------
    :isolation        Transaction isolation level (default is :default), which can be either of the following:
                       :default, :read-committed, :read-uncommitted, :repeatable-read, :serializable
    :propagation      Transaction propagation behavior (default is :required), which can be either of the following:
                       :mandatory, :nested, :never, :not-supported, :required, :requires-new, :supports
    :read-only?       Boolean (default false) - optimization parameter for the underlying transaction sub-system, which
                      may or may not respond to this parameter.
                      See: org.springframework.transaction.TransactionDefinition/isReadOnly
    :txn-name         Transaction name (String) that appears on the monitoring screen of the app server (e.g. WebLogic)
    :rollback-on      A predicate function that accepts the thrown exception (java.lang.Throwable) as the only argument
                      and returns true if the transaction should roll back, false otherwise.
                      Note: This argument is not allowed with `:rollback-for` or `:no-rollback-for`.
    :rollback-for     List containing exception classes or names as string on which transaction should be rolled back.
                      Note: This argument is not allowed with `:rollback-on`.
    :no-rollback-for  List containing exception Class objects/names as string on which transaction should NOT be rolled
                      back, but rather should be committed as it is. However, the exception will still be thrown after
                      the transaction is committed.
                      Note: This argument is not allowed with `:rollback-on`.
    :timeout-sec      Timeout (in seconds) for the transaction (only if underlying transaction sub-system supports it.)
                      See: org.springframework.transaction.TransactionDefinition/getTimeout"
  [txnspec] {:pre [(map? txnspec)
                   (instance? PlatformTransactionManager (:platform-txn-manager txnspec))]}
  (let [{:keys [^PlatformTransactionManager platform-txn-manager]} txnspec
        ^DefaultTransactionAttribute txn-attr (t/make-txndef txnspec)
        ^CustomTransactionTemplate txn-template (CustomTransactionTemplate. platform-txn-manager txn-attr)]
    (t/->TxnDef platform-txn-manager txn-attr txn-template)))
