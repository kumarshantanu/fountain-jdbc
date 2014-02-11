(ns fountain.jdbc.util
  (:import (clojure.lang Named)
           (java.util Collection List Map)
           (java.sql Connection PreparedStatement ResultSet ResultSetMetaData Statement)
           (org.springframework.dao.support DataAccessUtils)
           (org.springframework.jdbc.core ArgumentPreparedStatementSetter JdbcTemplate
                                          PreparedStatementCreator ResultSetExtractor)
           (org.springframework.jdbc.core.namedparam MapSqlParameterSource
                                                     NamedParameterJdbcTemplate)
           (org.springframework.jdbc.support JdbcUtils GeneratedKeyHolder KeyHolder)))


(defn expected-arg
  [expectation-msg x]
  (throw (IllegalArgumentException. (str "Expected " expectation-msg ", but found " (pr-str x)))))


(defn ^String as-string
  [x]
  (cond (string? x)         x
        (instance? Named x) (name x)
        :otherwise          (str x)))


(defn default-clj-to-db
  [iden]
  (-> (as-string iden)
    (.replace \- \_)))


(defn default-db-to-clj
  [^String iden]
  (let [^String lcase-iden (.toLowerCase iden)
        ^String valid-iden (.replace lcase-iden \_ \-)] 
    (keyword valid-iden)))


(defn ^"[Ljava.lang.Object;" as-object-array
  [x]
  (cond (nil? x)             (expected-arg "an array or a collection" x)
        (.isArray (class x)) (object-array x)
        (instance? List x)   (object-array x)
        :otherwise           (expected-arg "an array or a collection" x)))


(defn ^List as-list
  [x]
  (cond (instance? List x)   x
        (nil? x)             (expected-arg "a collection or an array" x)
        (.isArray (class x)) (vec x)
        :otherwise           (expected-arg "a collection or an array" x)))


(defn ^Map as-map
  [x]
  (if (instance? Map x) x
    (expected-arg "a map of named arguments" x)))


(defn ^Map map-keys
  [f m]
  (zipmap (map f (keys m))
          (vals m)))


(defn ^Map as-string-map
  [clj-to-db x]
  (->> (as-map x)
    (map-keys clj-to-db)))


(defn resultset-colcount
  "Given a ResultSet object, return a vector of column-count and
  ResultSetMetaData instance."
  [^ResultSet rs]
  (let [^ResultSetMetaData rsmd (.getMetaData rs)]
    [(.getColumnCount rsmd) rsmd]))


(defn resultset-colnames
  [db-to-clj col-count ^ResultSetMetaData rsmd]
  (->> (inc col-count)
    (range 1)
    (map #(db-to-clj (JdbcUtils/lookupColumnName rsmd %)))
    doall))


(defn resultset-colvalues
  [col-count ^ResultSet rs]
  (->> (inc col-count)
    (range 1)
    (map #(JdbcUtils/getResultSetValue rs %))
    doall))


(defn resultset-dataseq
  [col-count ^ResultSet rs]
  (let [rows (fn thisfn []
               (when (.next rs)
                 (cons (resultset-colvalues col-count rs)
                       (lazy-seq (thisfn)))))]
    (rows)))


(defn ^{:tag ResultSetExtractor :once true} make-resultset-extractor
  "Return instance of ResultSetExtractor. Arity-2 fn `f` should process the
  result-set statelessly without closing it."
  [db-to-clj f colnames?]
  (reify ResultSetExtractor
    (extractData [this ^ResultSet rs]
      (let [[col-count ^ResultSetMetaData rsmd] (resultset-colcount rs)]
        (f (if colnames? (resultset-colnames db-to-clj col-count rsmd) [])
           (resultset-dataseq col-count rs))))))


(defn ^:once realize-resultset
  [colnames dataseq]
  [colnames (doall dataseq)])


(defn realize-row
  [[colnames ^List rowlist]]
  [colnames (DataAccessUtils/requiredSingleResult rowlist)])


(defn ^PreparedStatementCreator make-pstmt-creator
  ([^String sql]
    (proxy [PreparedStatementCreator] []
     (createPreparedStatement [^Connection conn]
       (.prepareStatement conn sql Statement/RETURN_GENERATED_KEYS)))
    #_(reify PreparedStatementCreator
      (createPreparedStatement [this ^Connection conn]
         (let [^PreparedStatement ps (.prepareStatement conn sql Statement/RETURN_GENERATED_KEYS)]
          (println "***** Class of ps:" (class ps))
          ps))))
  ([^String sql args] {:pre [(not (map? args))]}
    (let [^ArgumentPreparedStatementSetter apss (ArgumentPreparedStatementSetter.
                                                  (as-object-array args))]
      (proxy [PreparedStatementCreator] []
        (createPreparedStatement [^Connection conn]
          (let [^PreparedStatement ps (.prepareStatement conn sql Statement/RETURN_GENERATED_KEYS)]
            (.setValues apss ps)
            ps)))
      #_(reify PreparedStatementCreator
         (createPreparedStatement [this ^Connection conn]
           (let [^PreparedStatement ps (.prepareStatement conn sql Statement/RETURN_GENERATED_KEYS)]
             (.setValues ps)
             ps)))
      )))


(defn ^MapSqlParameterSource make-map-param-source
  [args] {:pre [(map? args)]}
  (MapSqlParameterSource. (map-keys as-string args)))


(defn ^KeyHolder genkeys
  [f g]
  (let [^KeyHolder holder (GeneratedKeyHolder.)]
    (f holder)
    (g holder)))


(defn jt-genkey
  ([^JdbcTemplate jt ^String sql ^KeyHolder holder]
    (.update jt (make-pstmt-creator sql) holder))
  ([^JdbcTemplate jt ^String sql args ^KeyHolder holder]
    (.update jt (make-pstmt-creator sql args) holder)))

(defn njt-genkey
  [clj-to-db ^NamedParameterJdbcTemplate njt ^String sql args ^KeyHolder holder]
  (.update njt ^String sql
    ^MapSqlParameterSource (MapSqlParameterSource. (as-string-map clj-to-db args))
    holder))
