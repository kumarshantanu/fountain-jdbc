(ns org.bituf.fountain.transaction
  "Deal with Spring-JDBC transactions
  See also:
    Reference: http://static.springsource.org/spring/docs/3.0.x/reference/transaction.html
    API docs: http://static.springsource.org/spring/docs/3.0.x/javadoc-api/"
  (:import
    (java.util List Map)
    (javax.sql DataSource)
    (org.bituf.fountain.jdbc                     CustomTransactionTemplate)
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
                                                 TransactionAttribute))
  (:require
    [org.bituf.clj-miscutil :as mu]
    [org.bituf.clj-dbspec   :as sp]))


(def ^{:doc "Isolation Level keys"}
      isolation-keys [:default :read-committed :read-uncommitted
                      :repeatable-read :serializable])


(def ^{:doc "Isolation Levels"}
      isolation-levels (zipmap isolation-keys
                         [TransactionDefinition/ISOLATION_DEFAULT
                          TransactionDefinition/ISOLATION_READ_COMMITTED
                          TransactionDefinition/ISOLATION_READ_UNCOMMITTED
                          TransactionDefinition/ISOLATION_REPEATABLE_READ
                          TransactionDefinition/ISOLATION_SERIALIZABLE]))


(def ^{:doc "Propagation Behavior keys"}
      propagation-keys [:mandatory :nested :never :not-supported :required
                        :requires-new :supports])


(def ^{:doc "Propagation Behaviors"}
      propagation-behaviors (zipmap propagation-keys
                              [TransactionDefinition/PROPAGATION_MANDATORY
                               TransactionDefinition/PROPAGATION_NESTED
                               TransactionDefinition/PROPAGATION_NEVER
                               TransactionDefinition/PROPAGATION_NOT_SUPPORTED
                               TransactionDefinition/PROPAGATION_REQUIRED
                               TransactionDefinition/PROPAGATION_REQUIRES_NEW
                               TransactionDefinition/PROPAGATION_SUPPORTS]))


(def ^{:doc "PlatformTransaction manager key in DB-Spec"}
      TXNM-KEY :fountain.transaction.txnm)


(def ^{:doc "TransactionTemplate key in DB-Spec"}
      TXNT-KEY :fountain.transaction.txnt)


(def ^{:doc "TransactionStatus object for the current transaction."
       :tag TransactionStatus
       :dynamic true}
      *txn-status* nil)


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

    :read-only        Boolean (default false) - optimization parameter for the
                      underlying transaction sub-system, which may or may not
                      respond to this parameter. However, this flag will cause
                      :read-only to be turned on in the current DB-Spec.
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
  [& {:keys [isolation propagation read-only ^String txn-name rollback-on
             rollback-for no-rollback-for ^Integer timeout-sec]
      :or {isolation       nil ; :default
           propagation     nil ; :required
           read-only       nil ; false
           txn-name        nil ; nil (no name)
           rollback-on     nil ; e.g. #(instance? RuntimeException %)
           rollback-for    nil ; e.g. [RuntimeException Error]
           no-rollback-for nil ; e.g. [com.foo.business.NoInstrumentException]
           timeout-sec     nil ; TransactionDefinition/TIMEOUT_DEFAULT
           }
      :as opt}]
  {:post [(instance? DefaultTransactionAttribute %)]
   :pre  [(mu/verify-opt #{:isolation :propagation :read-only :txn-name
                           :rollback-on :rollback-for :no-rollback-for
                           :timeout-sec} opt)
          (mu/verify-arg (or (nil? isolation  ) (mu/contains-val? isolation-keys   isolation)))
          (mu/verify-arg (or (nil? propagation) (mu/contains-val? propagation-keys propagation)))
          (mu/verify-arg (or (nil? read-only  ) (mu/boolean? read-only)))
          (mu/verify-arg (or (nil? txn-name   ) (string? txn-name)))
          (mu/verify-arg (or (nil? rollback-on) (fn? rollback-on)))
          (mu/verify-arg (or (nil? rollback-for)
                           (and (coll? rollback-for)
                             (every? #(or (class? %) (string? %)) rollback-for))))
          (mu/verify-arg (or (nil? no-rollback-for)
                           (and (coll? no-rollback-for)
                             (every? #(or (class? %) (string? %)) no-rollback-for))))
          (mu/verify-arg (not (and rollback-on (or rollback-for no-rollback-for))))
          (mu/verify-arg (or (nil? timeout-sec) (mu/posnum? timeout-sec)
                           (= timeout-sec TransactionDefinition/TIMEOUT_DEFAULT)))]}
  (let [^DefaultTransactionAttribute
        td (cond
             ;; user has specified a predicate to handle exception
             (fn? rollback-on)  (proxy [DefaultTransactionAttribute] []
                                  (rollbackOn [^Throwable ex]
                                    (rollback-on ex)))
             ;; user specified list of exceptions to rollback on/not to rollback on
             (or rollback-for
               no-rollback-for) (let [^RuleBasedTransactionAttribute
                                      r (RuleBasedTransactionAttribute.)
                                      y (map #(if (class? %)
                                                (RollbackRuleAttribute. ^Class %)
                                                (RollbackRuleAttribute. ^String %))
                                          rollback-for)
                                      n (map #(if (class? %)
                                                (NoRollbackRuleAttribute. ^Class %)
                                                (NoRollbackRuleAttribute. ^String %))
                                          no-rollback-for)]
                                  (.setRollbackRules r (into y n))
                                  r)
             ;; user did not specify what to do on exception, so apply default
             ;; Note: DefaultTransactionAttribute rolls back only on unchecked
             ;;       exceptions (RuntimeException, Error). Since it is not
             ;;       mandatory to handle checked exceptions in Clojure we must
             ;;       rollback on all exceptions without discrimination.
             :else                (proxy [DefaultTransactionAttribute] []
                                    (rollbackOn [^Throwable ex]
                                      true)))]
    (if isolation   (.setIsolationLevel      td ^Integer (get isolation-levels      isolation)))
    (if propagation (.setPropagationBehavior td ^Integer (get propagation-behaviors propagation)))
    (if read-only   (.setReadOnly td true) (.setReadOnly td false))
    (if txn-name    (.setName     td txn-name))
    (if timeout-sec (.setTimeout  td timeout-sec))
    td))


(defn ^PlatformTransactionManager
       get-txnm
  "Get PlatformTransactionManager instance from DB-Spec if available"
  ([spec] {:pre [(map? spec)]}
    (TXNM-KEY spec))
  ([] (get-txnm sp/*dbspec*)))


(defn ^CustomTransactionTemplate ; ^TransactionTemplate
       get-txnt
  "Get TransactionTemplate instance from DB-Spec if available"
  ([spec] {:pre [(map? spec)]}
    (TXNT-KEY spec))
  ([] (get-txnt sp/*dbspec*)))


(defn get-txn
  "Get current transaction object (create if necessary) that can be used for
  commit or rollback.
  See also:
    org.springframework.transaction.PlatformTransactionManager/getTransaction"
  ([^PlatformTransactionManager txnm ^TransactionDefinition txndef]
    {:pre [(instance? PlatformTransactionManager txnm)
           (instance? TransactionDefinition      txndef)]}
    (.getTransaction txnm txndef))
  ([^TransactionDefinition txndef]
    {:pre [(instance? TransactionDefinition      txndef)]}
    (get-txn (get-txnm) txndef))
  ([]
    (get-txn (get-txnm) (get-txnt))))


(defn commit
  "Commit given/current transaction."
  ([^PlatformTransactionManager txnm ^TransactionStatus status]
    {:pre [(instance? PlatformTransactionManager txnm)
           (instance? TransactionStatus          status)]}
    (.commit txnm status))
  ([^TransactionStatus status]
    {:pre [(instance? TransactionStatus          status)]}
    (commit (get-txnm) status))
  ([]
    (commit (get-txnm) *txn-status*)))


(defn rollback
  "Rollback given/current transaction."
  ([^PlatformTransactionManager txnm ^TransactionStatus status]
    {:pre [(instance? PlatformTransactionManager txnm)
           (instance? TransactionStatus          status)]}
    (.rollback txnm status))
  ([^TransactionStatus status]
    {:pre [(instance? TransactionStatus status)]}
    (rollback (get-txnm) status))
  ([]
    (rollback (get-txnm) *txn-status*)))


(defn make-txntspec
  "Return a map with the following key associated to its respective value:
    :fountain.transaction.txnm - PlatformTransactionManager instance
    :fountain.transaction.txnt - TransactionTemplate instance
    :read-only                 - Only from a specified transaction definition
  See also:
    with-context
    clj-dbspec/*dbspec*"
  [& {:keys [^DataSource datasource ^TransactionAttribute ; ^TransactionDefinition
                                     txndef]
      :or   {datasource  nil
             txndef      nil}
      :as opt}]
  {:post [(mu/verify-cond (map? %))
          (mu/verify-cond (contains? % TXNT-KEY))]
   :pre  [(mu/verify-opt #{:datasource :txndef} opt)
          (mu/verify-arg (or (nil? datasource) (instance? DataSource datasource)))
          (mu/verify-arg (or (nil? txndef) (instance? TransactionAttribute ; TransactionDefinition
                                             txndef)))]}
  (let [ds   (or datasource (:datasource sp/*dbspec*)
               (mu/illegal-arg "No valid DataSource found/supplied"))
        txnm (DataSourceTransactionManager. ^DataSource ds)
        txro (if txndef (.isReadOnly txndef) nil)
        txnd (or txndef (make-txndef))
        txnt (CustomTransactionTemplate. ; TransactionTemplate.
                 ^PlatformTransactionManager txnm txnd)]
     (if (nil? txro) {TXNM-KEY txnm
                      TXNT-KEY txnt}
       {:read-only txro
        TXNM-KEY   txnm
        TXNT-KEY   txnt})))


(defn assoc-txnt
  "Associate TransactionTemplate instance with DB-Spec. `txntspec` is either a
  spec (map) or is a no-arg function that returns txn-template spec.
  See also:
    make-txntspec"
  ([spec txntspec] {:post [(mu/verify-cond (map? %))]
                    :pre  [(mu/verify-arg (map? spec))
                           (mu/verify-arg (or (map? txntspec)
                                            (fn? txntspec)))]}
    (let [tspec (sp/with-dbspec spec
                  (sp/value txntspec))]
      (mu/verify-cond (map? tspec))
      (mu/verify-cond (contains? tspec TXNT-KEY))
      (sp/assoc-kvmap spec tspec)))
  ([spec] {:post [(mu/verify-cond (map? %))]
           :pre  [(mu/verify-arg (map? spec))
                  (mu/verify-arg (contains? spec :datasource))
                  (mu/verify-arg (instance? DataSource (:datasource spec)))]}
    (assoc-txnt spec make-txntspec)))


(defn wrap-txncallback
  "Create TransactionTemplate instance and putting into clj-dbspec/*dbspec* as a
  key execute f."
  ([txntspec f] {:post [(fn? %)]
                 :pre  [(and (map? txntspec) (contains? txntspec
                                               TXNT-KEY))
                        (fn? f)]}
    (fn [& args]
      (let [ts txntspec
            tt ^CustomTransactionTemplate ; ^TransactionTemplate
                (TXNT-KEY ts)
            wf (sp/wrap-dbspec ts
                 (fn [& args]
                   (.execute tt (reify TransactionCallback
                                  (doInTransaction [this ^TransactionStatus status]
                                    (binding [*txn-status* status]
                                      (apply f args)))))))]
        (apply wf args))))
  ([f] {:post [(fn? %)]
        :pre  [(fn? f)]}
    (if (get-txnt) f
      (wrap-txncallback (make-txntspec) f))))
