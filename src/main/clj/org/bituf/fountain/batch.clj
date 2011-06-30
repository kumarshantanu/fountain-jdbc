(ns org.bituf.fountain.batch
  (:require
    [clojure.pprint :as pp]
    [org.bituf.clj-dbspec    :as sp]
    [org.bituf.clj-miscutil  :as mu]
    [org.bituf.fountain.jdbc :as jd])
  (:import
    (java.sql   PreparedStatement)
    (javax.sql  DataSource)
    (org.springframework.jdbc.core                   ColumnMapRowMapper
                                                     PreparedStatementSetter)
    (org.springframework.jdbc.core.namedparam        NamedParameterUtils)
    (org.springframework.batch.item                  ExecutionContext
                                                     ItemReader)
    (org.springframework.batch.item.database         JdbcCursorItemReader
                                                     JdbcPagingItemReader)
    (org.springframework.batch.item.database.support SqlPagingQueryProviderFactoryBean)))


(defn row-seq
  "Return a lazy sequence of database rows. Each row is a map of column names
  to respective values."
  [reader-fn]
  (let [db-to-clj (:db-to-clj sp/*dbspec*)]
    (map (fn [row] (mu/map-keys db-to-clj row))
         (take-while mu/not-nil? (mu/repeat-exec reader-fn)))))


(defn ^PreparedStatementSetter
       make-prepared-statement-setter
  "Return a PreparedStatementSetter object for given args"
  [arg-coll] {:pre [(or (coll? arg-coll)
                        (mu/array? arg-coll))]}
  (proxy [PreparedStatementSetter] []
    (setValues [^PreparedStatement ps]
               (doall (map (fn [idx arg]
                             (println (format "Setting arg %d = %s" idx arg))
                             (.setObject ps idx arg))
                           (iterate inc 1) arg-coll)))))


(defn jdbc-cursor-reader
  "Return a no-arg reader function that implicitly executes given SQL statement
  and can be used to read rows from the ResultSet, where each row is a map of
  column-names to respective values.
  Example:
    (println (row-seq (jdbc-cursor-reader \"SELECT * FROM emp\")))
    ;; OR
    (with-open [r (JdbcCursorItemReader.)]
      (println (row-seq (jdbc-cursor-reader \"SELECT * FROM emp\"
                          :jci-reader r))))
  See also:
    row-seq"
  [sql
   & {:keys [datasource
             jci-reader      ; JdbcCursorItemReader object (default: creates new)
             reader-fn       ; 1-arg fn to read from ItemReader (default: re-throws exception)
             ignore-warnings ; boolean
             fetch-size      ; int (0 = ignored by driver)
             max-rows        ; int (0 = no limit)
             query-timeout   ; int (seconds)
             verify-cursor-position ; boolean (default: true, should be false)
             save-state      ; boolean (default: true)
             driver-supports-absolute ; boolean (default: false)
             use-shared-extended-connection ; boolean (default: false)
             ]
      :as opt}]
  {:post [(mu/verify-cond (fn %))]
   :pre  [(mu/verify-opt #{:datasource :jci-reader :reader-fn :ignore-warnings
                           :fetch-size :max-rows :query-timeout
                           :verify-cursor-position :save-state
                           :driver-supports-absolute
                           :use-shared-extended-connection} opt)]}
  (let [ds (or datasource (:datasource sp/*dbspec*))
        ^JdbcCursorItemReader
        ir (or jci-reader (JdbcCursorItemReader.))
        [sql & args] (mu/as-vector sql)
        arg-1        (first args)
        arg-map      (and (= 1 (count args)) (map? arg-1) arg-1)]
    (doto ir
      (.setDataSource ds)
      (.setSql sql)
      (.setRowMapper (ColumnMapRowMapper.)))
    (cond
      ;; named parameters
      arg-map               (let [sql-jdbc  (NamedParameterUtils/parseSqlStatementIntoString sql)
                                  arg-array (NamedParameterUtils/buildValueArray
                                              sql (mu/map-keys (:clj-to-db sp/*dbspec*) arg-map))]
                              (doto ir
                                (.setSql sql-jdbc)
                                (.setPreparedStatementSetter
                                  (make-prepared-statement-setter arg-array))))
      ;; positional parameters
      (mu/not-empty? args)  (do
                              (.setPreparedStatementSetter
                                ir (make-prepared-statement-setter args))))
    (let [fetch-size    (or fetch-size    (:fetch-size    sp/*dbspec*))
          query-timeout (or query-timeout (:query-timeout sp/*dbspec*))]
      (when (mu/not-nil? ignore-warnings)                (.setIgnoreWarnings              ir ignore-warnings))
      (when (mu/not-nil? fetch-size)                     (.setFetchSize                   ir fetch-size))
      (when (mu/not-nil? max-rows)                       (.setMaxRows                     ir max-rows))
      (when (mu/not-nil? query-timeout)                  (.setQueryTimeout                ir query-timeout))
      (when (mu/not-nil? verify-cursor-position)         (.setVerifyCursorPosition        ir verify-cursor-position))
      (when (mu/not-nil? save-state)                     (.setSaveState                   ir save-state))
      (when (mu/not-nil? driver-supports-absolute)       (.setDriverSupportsAbsolute      ir driver-supports-absolute))
      (when (mu/not-nil? use-shared-extended-connection) (.setUseSharedExtendedConnection ir use-shared-extended-connection)))
    ;; open reader
    (.open ir (ExecutionContext.))
    (jd/show-sql sql)
    (partial
      (or reader-fn (fn [^ItemReader r] (.read r)))
      ir)))


(defrecord SelectCriteria [select    ; "SELECT DISTINCT e.name, e.born_on, d.name as dept_name"
                           from      ; "FROM emp e, dept d"
                           where     ; "WHERE e.name NOT LIKE 'S%'"
                           sort-key  ; "dept_name, born_on"
                           sort-asc  ; true
                           args])


(defn nil-or?
  [preds x]
  {:pre [(or (fn? preds)
             (and (coll? preds)
                  (every? fn? preds)))]}
  (or (nil? x)
      (some #(% x) (mu/as-vector preds))))


(defn make-criteia
  [& {:keys [select from where sort-key sort-asc args]
      :as criteria}]
  {:pre [(mu/verify-arg (string?     select))
         (mu/verify-arg (string?     from))
         (mu/verify-arg (nil-or? string?     where))
         (mu/verify-arg (string?     sort-key))
         (mu/verify-arg (nil-or? mu/boolean? sort-asc))
         (mu/verify-arg (nil-or? coll?       args))]}
  (SelectCriteria. select from where sort-key (if (nil? sort-asc) true sort-asc)
                   args))


(defn select-criteria?
  [x]
  (instance? SelectCriteria x))


(defn ^JdbcPagingItemReader set-range!
  "Set `start` (1 based; inclusive) and `stop` (1 based; exclusive) range for a
  JdbcPagingItemReader to fetch rows from."
  [^JdbcPagingItemReader jpi-reader start stop]
  {:pre [(mu/verify-arg (instance? JdbcPagingItemReader jpi-reader))
         (mu/verify-arg (mu/posnum? start))
         (mu/verify-arg (mu/posnum? stop))
         (mu/verify-arg (<= start stop))]}
  (doto jpi-reader
    (.setCurrentItemCount start)
    (.setMaxItemCount     stop))
  jpi-reader)


(defn ^JdbcPagingItemReader set-page!
  "Set `page-size` and `which-page` (1 based) to read next from a
  JdbcPagingItemReader object."
  [^JdbcPagingItemReader jpi-reader page-size current-page]
  {:pre [(mu/verify-arg (instance? JdbcPagingItemReader jpi-reader))
         (mu/verify-arg (mu/posnum? page-size))
         (mu/verify-arg (mu/posnum? current-page))]}
  (let [start-pos (inc (* page-size (dec current-page)))
        max-count (+ start-pos page-size)]
    (set-range! jpi-reader start-pos max-count)))


(defn jdbc-paging-reader
  [^SelectCriteria criteria
   & {:keys [datasource
             jpi-reader  ; instance of JdbcPagingItemReader
             reader-fn   ; 1-arg fn to read from ItemReader (default: re-throws exception)
             fetch-size  ; int
             save-state
             start-pos   ; starting row position for reading (pagination)
             max-count   ; max row count to fetch (pagination)
             ]
      :as opt}]
  {:post [(mu/verify-cond (fn? %))]
   :pre [(mu/verify-opt #{:datasource  :jpi-reader :reader-fn
                           :fetch-size :max-rows   :query-timeout
                           :verify-cursor-position :save-state
                           :start-pos  :max-count} opt)
         (mu/verify-arg (select-criteria?   criteria))
         (mu/verify-arg (string? (:select   criteria)))
         (mu/verify-arg (string? (:from     criteria)))
         (mu/verify-arg (string? (:sort-key criteria)))
         ;; non-criteria args
         (mu/verify-arg (nil-or? #(instance? DataSource %) datasource))
         (mu/verify-arg (nil-or? fn?         reader-fn))
         (mu/verify-arg (nil-or? number?     fetch-size))
         (mu/verify-arg (nil-or? mu/boolean? save-state))
         (mu/verify-arg (nil-or? integer?    start-pos))
         (mu/verify-arg (nil-or? integer?    max-count))]}
  (let [ds (or datasource (:datasource sp/*dbspec*))
        ^JdbcPagingItemReader
        ir (or jpi-reader (JdbcPagingItemReader.))
        qf (SqlPagingQueryProviderFactoryBean.)]
    (doto qf
      (.setDataSource   ds)
      (.setSelectClause (:select   criteria))
      (.setFromClause   (:from     criteria))
      (.setSortKey      (:sort-key criteria)))
    (let [wh (:where    criteria)] (when wh (.setWhereClause qf wh)))
    (let [sa (:sort-asc criteria)] (when (mu/not-nil? sa)
                                     (.setAscending qf sa)))
    (doto ir
      (.setDataSource ds)
      (.setRowMapper (ColumnMapRowMapper.))
      (.setQueryProvider (.getObject qf)))
    ;; query parameters
    (when-let [args (:args criteria)]
      (let [arg-map (if (map? args)
                      (mu/map-keys (:clj-to-db sp/*dbspec*) args)
                      (zipmap (map str (range 1 (inc (count args)))) args))]
        (.setParameterValues ir arg-map)))
    ; opt params
    (let []
      (when (mu/not-nil? fetch-size) (.setFetchSize ir fetch-size))
      (when (mu/not-nil? save-state) (.setSaveState ir save-state))
      (when (mu/not-nil? start-pos)  (.setCurrentItemCount ir start-pos))
      (when (mu/not-nil? max-count)  (.setMaxItemCount     ir max-count)))
    ;; open reader
    (.afterPropertiesSet ir)
    (.open ir (ExecutionContext.))
    (jd/show-sql criteria)
    (partial
      (or reader-fn (fn [^ItemReader r] (.read r)))
      ir)))