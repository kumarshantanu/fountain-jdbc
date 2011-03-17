(ns org.bituf.fountain.jdbc
  "Functions that deal with SpringJDBC classes and functions. Note that calls
  related to SimpleJdbcInsert and SimpleJdbcCall use database/JDBC metadata -
  you can use them only if your database and the JDBC driver support them."
  (:import
    (java.util List Map)
    (javax.sql DataSource)
    (org.springframework.jdbc.core.simple SimpleJdbcTemplate SimpleJdbcInsert
                                          SimpleJdbcCall)
    (org.springframework.jdbc.support     KeyHolder))
  (:require
    [org.bituf.clj-miscutil :as mu]
    [org.bituf.clj-dbspec   :as sp]))


(def ^{:doc "Fountain-JDBC version (only major and minor)"}
      version [0 2])


(defn make-sjtspec
  "Return a map with the following key associated to its respective value:
    :org.bituf.fountain.jdbc.sjt - SimpleJdbcTemplate
  See also:
    with-context
    clj-dbspec/*dbspec*"
  [& {:keys [^DataSource datasource]
      :or   {datasource  nil}}]
  (let [ds  (or datasource (:datasource sp/*dbspec*)
              (mu/illegal-arg "No valid DataSource found/supplied"))
        sjt ^SimpleJdbcTemplate (SimpleJdbcTemplate. ^DataSource ds)]
     {:fountain.jdbc.sjt sjt}))


(defn wrap-sjt
  "Create SimpleJdbcTemplate instance and putting into clj-dbspec/*dbspec* as a
  key execute f."
  [f] {:post [(fn? %)]
       :pre  [(fn? f)]}
  (fn [& args]
    (let [wf (sp/wrap-dbspec (make-sjtspec)
               f)]
      (apply wf args))))


(defn- ^SimpleJdbcTemplate get-sjt
  "Get SimpleJdbcTemplate from context"
  ([context] (:fountain.jdbc.sjt context))
  ([] (get-sjt sp/*dbspec*)))


(defn- ^SimpleJdbcInsert get-sji
  "Get SimpleJdbcInsert from context"
  ([context] (:fountain.jdbc.sji context))
  ([] (get-sji sp/*dbspec*)))


(defn show-sql
  "Print the SQL statement if the *show-sql* flag is true."
  [sql]
  (when (:show-sql sp/*dbspec*)
    ((:show-sql-fn sp/*dbspec*) sql)))


(defmacro with-query-args
  "Transform 'args' as suitable for SimpleJdbcTemplate method calls and bind
  to 'qargs' with respective type hints, then call body of code in that context.
  The two kinds of args supported are:
  1. a map <String key, ? param value> (for SQL with named parameters), or
  2. an array (for SQL with standard ? placeholders)
  See also:
  1. http://kotka.de/blog/2009/12/with-meta_and_the_reader.html
  2. http://j.mp/bO0nmR (Clojure Google group)"
  [[qargs args] & body]
  `(cond
     (map?      ~args) (let [~qargs (mu/keys-to-str ~args)]  ; Map<String, ?>
                         ~@body)
     (coll?     ~args) (let [~(with-meta qargs ; Object[]
                                {:tag "[Ljava.lang.Object;"}) (into-array ~args)]
                         ~@body)
     (mu/array? ~args) (let [~(with-meta qargs ; Object[]
                                {:tag "[Ljava.lang.Object;"}) ~args]
                         ~@body)
     :else (mu/illegal-argval "args"
             "either map, or collection, or array" ~args)))


(defn query-for-int
  "Execute query and return integer value."
  ([^String sql args]
    (with-query-args [qargs args]
      (show-sql sql)
      (.queryForInt (get-sjt) sql qargs)))
  ([sql]
    (query-for-int sql {})))


(defn query-for-long
  "Execute query and return long value."
  ([^String sql args]
    (with-query-args [qargs args]
      (show-sql sql)
      (.queryForLong (get-sjt) sql qargs)))
  ([sql]
    (query-for-long sql {})))


(defn query-for-map
  "Execute query and return a row (expressed as a map)."
  ([^String sql args]
    (with-query-args [qargs args]
      (mu/map-keys (:db-to-clj sp/*dbspec*)
        (do
          (show-sql sql)
          (.queryForMap (get-sjt) sql qargs)))))
  ([sql]
    (query-for-map sql {})))


(defn query-for-list
  "Execute query and return a lazy list of rows (each row is a map)."
  ([^String sql args]
    (with-query-args [qargs args]
      (map #(mu/map-keys (:db-to-clj sp/*dbspec*) %)
        (do
          (show-sql sql)
          (.queryForList (get-sjt) sql qargs)))))
  ([sql]
    (query-for-list sql {})))


(defn update
  "Execute update-query and return integer result (number of rows affected)."
  ([^String sql args]
    (with-query-args [qargs args]
      (show-sql sql)
      (.update (get-sjt) sql qargs)))
  ([sql]
    (update sql {})))


(defn batch-update
  "Execute update-query in a batch using the query parameters. 'batch-args' is
  1. either a list of argument lists
  2. or a list of named param-value maps
  Return a lazy list of integers, each being the number of rows affected."
  [^String sql batch-args]
  (let [args1        (first batch-args)
        is-map       (map? args1)
        is-seq       (or (coll? args1) (mu/array? args1))
        show-sql-fn  #(show-sql (format "(Batch-size: %d) -- %s"
                                  (count batch-args) sql))
        result-array (cond ; returns an int array, or throws exception
                       is-map (let [^"[Ljava.util.Map;" args-array ; Map<String, ?>[]
                                    (into-array (map mu/keys-to-str batch-args))]
                                (show-sql-fn)
                                (.batchUpdate (get-sjt) sql args-array))
                       is-seq (let [^List args-list  ; List<Object[]>
                                    (map into-array batch-args)]
                                (show-sql-fn)
                                (.batchUpdate (get-sjt) sql args-list))
                       :else (mu/illegal-argval "batch-args"
                               "either list of lists, or list of maps"
                               batch-args))]
    (map identity result-array)))


;; -------------------------
;;    METADATA  functions
;;
;; Functions that take advantage of database metadata to limit the amount of
;; configuration needed - they work with SimpleJdbcInsert and SimpleJdbcCall.
;; -------------------------

(defn ^SimpleJdbcInsert make-sji
  "Create an instance of org.springframework.jdbc.core.simple.SimpleJdbcInsert
  based on given arguments and return the same. The instance is thread-safe and
  can be re-used across any number of calls.
  Arguments:
    table-name  (Clojure form) database table name to insert row(s) into
  Optional arguments:
    :datasource (DataSource, default Clj-DBSpec/*dbspec*) data source
    :gencols    (collection) column names with generated values
    :catalog    (Clojure form, default Clj-DBSpec/*dbspec*) catalog name
    :schema     (Clojure form, default Clj-DBSpec/*dbspec*) schema name
    :use-meta   (Boolean, default true) whether to use database metadata"
  [table-name
   & {:keys [^DataSource datasource gencols ^String catalog ^String schema
             use-meta]
      :or   {datasource  (:datasource sp/*dbspec*)
             gencols  []
             catalog  (:catalog sp/*dbspec*)
             schema   (:schema  sp/*dbspec*)
             use-meta true}}]
  (let [v-gencols (mu/as-vector gencols)]
    (-> (SimpleJdbcInsert. datasource)
      (.withTableName (sp/db-iden table-name))
      (#(if (mu/not-empty? v-gencols)
          (.usingGeneratedKeyColumns ^SimpleJdbcInsert %
            ^"[Ljava.lang.String;" (into-array String
                                     (map sp/db-iden v-gencols))) %))
      (#(if schema   (.withSchemaName  ^SimpleJdbcInsert % schema)  %))
      (#(if catalog  (.withCatalogName ^SimpleJdbcInsert % catalog) %))
      (#(if use-meta %  ; returns SimpleJdbcInsertOperations
          (.withoutTableColumnMetaDataAccess ^SimpleJdbcInsert %))))))


;; -----------------------
;;    INSERT  functions
;; -----------------------


(defn show-insert-sql
  "Print the Insert SQL."
  [msg ^SimpleJdbcInsert sji]
  (show-sql (str msg " - " (.getInsertString sji))))


(defn insert
  "Insert row and return the number (int) of affected rows. This function is not
  suitable if you want to retrieve generated column keys.
  See also: insert-give-id, insert-give-idmap"
  [^SimpleJdbcInsert sji row]
  ;; sji.withTableName("tableName")
  ;; .execute(/* Map<String, Object> */ row); // int
  (try
    (.execute sji ^Map (mu/map-keys sp/db-iden row))
    (finally
      (mu/maybe (show-insert-sql "Returning affected row count: " sji)))))


(defn insert-give-id
  "Insert row and return generated ID."
  [^SimpleJdbcInsert sji row]
  ;; sji.withTableName("tableName")
  ;; .executeAndReturnKey(/* Map<String, Object> */ row); // Number
  (try
    (.executeAndReturnKey sji ^Map (mu/map-keys sp/db-iden row))
    (finally
      (mu/maybe (show-insert-sql "Returning generated ID: " sji)))))


(defn insert-give-idmap
  "Insert row and return generated ID map (for multiple columns)."
  [^SimpleJdbcInsert sji row]
  ;; sji.withTableName("tableName")
  ;; .executeAndReturnKeyHolder(/* Map<String, Object> */ row)
  ;; .getKeys(); // Map<String, Object>
  (try
    (mu/map-keys sp/clj-iden
      (.getKeys ^KeyHolder (.executeAndReturnKeyHolder sji
                             ^Map (mu/map-keys sp/db-iden row))))
    (finally
      (mu/maybe (show-insert-sql "Returning generated ID Map: " sji)))))


(defn insert-batch
  "Insert rows in a batch and return a lazy list containing number of affected
  rows per insertion."
  [^SimpleJdbcInsert sji batch-rows]
  ;; sji.withTableName("tableName")
  ;; .executeBatch(/* Map<String, Object>[] */ batchRows); // int[]
  (try
    (->> batch-rows
      (map #(mu/map-keys sp/db-iden %))
      ^"[Ljava.util.Map;" into-array
      (.executeBatch sji)
      (map identity))
    (finally
      (mu/maybe (show-insert-sql (format "Batch INSERT of size %d"
                                   (count batch-rows)) sji)))))
