(ns fountain.jdbc.impl-txn
  (:require [fountain.jdbc :as j])
  (:import
    (java.util List Map)
    (javax.sql DataSource)
    (fountain.jdbc                               CustomTransactionTemplate Txn TxnAccess)
    (org.springframework.jdbc.datasource         DataSourceTransactionManager)
    (org.springframework.transaction             PlatformTransactionManager
                                                 TransactionDefinition
                                                 TransactionStatus)
    (org.springframework.transaction.support     DefaultTransactionDefinition
                                                 TransactionCallback
                                                 ;TransactionTemplate
                                                 )
    (org.springframework.transaction.interceptor DefaultTransactionAttribute
                                                 NoRollbackRuleAttribute
                                                 RollbackRuleAttribute
                                                 RuleBasedTransactionAttribute
                                                 TransactionAttribute)))


(def ^{:doc "Isolation Level keys"} isolation-keys
  [:default :read-committed :read-uncommitted
   :repeatable-read :serializable])


(def ^{:doc "Isolation Levels"} isolation-levels
  (zipmap isolation-keys
          [TransactionDefinition/ISOLATION_DEFAULT
           TransactionDefinition/ISOLATION_READ_COMMITTED
           TransactionDefinition/ISOLATION_READ_UNCOMMITTED
           TransactionDefinition/ISOLATION_REPEATABLE_READ
           TransactionDefinition/ISOLATION_SERIALIZABLE]))


(def ^{:doc "Propagation Behavior keys"} propagation-keys
  [:mandatory :nested :never :not-supported :required
   :requires-new :supports])


(def ^{:doc "Propagation Behaviors"} propagation-behaviors
  (zipmap propagation-keys
          [TransactionDefinition/PROPAGATION_MANDATORY
           TransactionDefinition/PROPAGATION_NESTED
           TransactionDefinition/PROPAGATION_NEVER
           TransactionDefinition/PROPAGATION_NOT_SUPPORTED
           TransactionDefinition/PROPAGATION_REQUIRED
           TransactionDefinition/PROPAGATION_REQUIRES_NEW
           TransactionDefinition/PROPAGATION_SUPPORTS]))


(defn ^DefaultTransactionAttribute make-txndef
  "Return constraints based TransactionDefinition implementation.
  Optional args:
    :isolation        Transaction isolation level (default is :default), which
                      can be either of the following:
                       :default, :read-committed, :read-uncommitted,
                       :repeatable-read, :serializable
    :propagation      Transaction propagation behavior (default is :required),
                      which can be either of the following:
                       :mandatory, :nested, :never, :not-supported,
                       :required, :requires-new, :supports
    :read-only?       Boolean (default false) - optimization parameter for the
                      underlying transaction sub-system, which may or may not
                      respond to this parameter.
                      See: org.springframework.transaction.TransactionDefinition/isReadOnly
    :txn-name         Transaction name (String) that shows up on the monitoring
                      console of the application server (e.g. on WebLogic)
    :rollback-on      A predicate function that accepts the thrown exception
                      (java.lang.Throwable) as the only argument and returns
                      true if the transaction should roll back, false otherwise.
                      Note: This argument is not allowed with `:rollback-for` or
                      `:no-rollback-for`.
    :rollback-for     List containing exception Class objects/names as string
                      on which transaction should be rolled back.
                      Note: This argument is not allowed with `:rollback-on`.
    :no-rollback-for  List containing exception Class objects/names as string
                      on which transaction should NOT be rolled back, but rather
                      should be committed as it is. However, the exception will
                      still be thrown after the transaction is committed.
                      Note: This argument is not allowed with `:rollback-on`.
    :timeout-sec      Timeout (in seconds) for the transaction (only if the
                      underlying transaction sub-system supports it.)
                      See: org.springframework.transaction.TransactionDefinition/getTimeout
  See also:
    http://static.springsource.org/spring/docs/3.0.x/javadoc-api/org/springframework/transaction/interceptor/RollbackRuleAttribute.html
    Short URL: http://j.mp/ftIHsM"
  [{:keys [isolation propagation read-only? ^String txn-name rollback-on
             rollback-for no-rollback-for ^Integer timeout-sec]
      :or {isolation       nil ; :default
           propagation     nil ; :required
           read-only?      nil ; false
           txn-name        nil ; nil (no name)
           rollback-on     nil ; e.g. #(instance? RuntimeException %)
           rollback-for    nil ; e.g. [RuntimeException Error]
           no-rollback-for nil ; e.g. [com.foo.business.NoInstrumentException]
           timeout-sec     nil ; TransactionDefinition/TIMEOUT_DEFAULT
           }
      :as opt}]
  (let [expected-class-or-str #(throw (IllegalArgumentException.
                                        (format "Expected class or string, but found: (%s) %s" (class %) (pr-str %))))
        ^DefaultTransactionAttribute td
        (cond
          ;; user has specified a predicate to handle exception
          (fn? rollback-on)  (proxy [DefaultTransactionAttribute] []
                               (rollbackOn [^Throwable ex]
                                 (rollback-on ex)))
          rollback-on        (throw (IllegalArgumentException.
                                      (str "Expected `rollback-on` to be a fn, but found: " (pr-str rollback-on))))
          ;; user specified list of exceptions to rollback on/not to rollback on
          (or rollback-for
              no-rollback-for) (let [^RuleBasedTransactionAttribute r (RuleBasedTransactionAttribute.)
                                     y (map #(cond
                                               (class? %) (RollbackRuleAttribute. ^Class %)
                                               (string? %) (RollbackRuleAttribute. ^String %)
                                               :otherwise (expected-class-or-str %))
                                            rollback-for)
                                     n (map #(cond
                                               (class? %) (NoRollbackRuleAttribute. ^Class %)
                                               (string? %) (NoRollbackRuleAttribute. ^String %)
                                               :otherwise (expected-class-or-str %))
                                            no-rollback-for)]
                                 (.setRollbackRules r (into (doall y) (doall n)))
                                 r)
          ;; user did not specify what to do on exception, so apply default
          ;; Note: DefaultTransactionAttribute rolls back only on unchecked
          ;;       exceptions (RuntimeException, Error). Since it is not
          ;;       mandatory to handle checked exceptions in Clojure we must
          ;;       rollback on all exceptions without discrimination.
          :else                (proxy [DefaultTransactionAttribute] []
                                 (rollbackOn [^Throwable ex]
                                   true)))]
    (when isolation   (.setIsolationLevel      td ^Integer (get isolation-levels      isolation)))
    (when propagation (.setPropagationBehavior td ^Integer (get propagation-behaviors propagation)))
    (if read-only?  (.setReadOnly td true) (.setReadOnly td false))
    (when txn-name    (.setName     td txn-name))
    (when timeout-sec (.setTimeout  td timeout-sec))
    td))


(defrecord ActiveTxn [^PlatformTransactionManager platform-txn-manager ^TransactionStatus txn-status]
  Txn
  (commit   [this] (.commit platform-txn-manager txn-status))
  (rollback [this] (.rollback platform-txn-manager txn-status)))


(defrecord TxnDef [^PlatformTransactionManager platform-txn-manager ^TransactionAttribute txn-attr
                   ^CustomTransactionTemplate txn-template]
  TxnAccess
  (get-txn [this] (->> txn-attr
                    (.getTransaction platform-txn-manager)
                    (ActiveTxn. platform-txn-manager)))
  (in-txn  [this f] (.execute txn-template (reify TransactionCallback
                                             (doInTransaction [this ^TransactionStatus txn-status]
                                               (f))))))
