(ns fountain.jdbc.impl-ops
  (:require [fountain.jdbc      :as j]
            [fountain.jdbc.util :as u])
  (:import (fountain.jdbc Invoke Ops)
           (java.util List Map)
           (java.sql ResultSet)
           (javax.sql DataSource)
           (org.springframework.dao.support DataAccessUtils)
           (org.springframework.jdbc.core JdbcTemplate RowMapper)
           (org.springframework.jdbc.core.namedparam MapSqlParameterSource
                                                     NamedParameterJdbcTemplate)
           (org.springframework.jdbc.support GeneratedKeyHolder KeyHolder)))


(defn ^Ops mock-ops-impl
  "Given arity-0 mock fn `f`, return Ops instance that only invokes (f) on every protocol fn call."
  [g]
  (reify j/Ops
    (batch-update   [this sql-coll]       (g))
    (batch-update   [this sql args-coll]  (g))
    (execute        [this sql]            (g))
    (lazy-query     [this f cn? sql]      (g))
    (lazy-query     [this f cn? sql args] (g))
    (query-datalist [this sql]            (g))
    (query-datalist [this sql args]       (g))
    (query-datarow  [this sql]            (g))
    (query-datarow  [this sql args]       (g))
    (query-for-list [this sql]            (g))
    (query-for-list [this sql args]       (g))
    (query-for-row  [this sql]            (g))
    (query-for-row  [this sql args]       (g))
    (query-for-val  [this sql]            (g))
    (query-for-val  [this sql args]       (g))
    (update         [this sql]            (g))
    (update         [this sql args]       (g))
    (genkey         [this sql]            (g))
    (genkey         [this sql args]       (g))
    (genkey-via     [this f sql]          (g))
    (genkey-via     [this f sql args]     (g))))


(defn ^JdbcTemplate make-jt
  [^DataSource ds {:keys [fetch-size query-timeout max-rows] :as opts}]
  (let [^JdbcTemplate jt (JdbcTemplate. ^DataSource ds)]
    (when fetch-size    (.setFetchSize    jt fetch-size))
    (when query-timeout (.setQueryTimeout jt query-timeout))
    (when max-rows      (.setMaxRows      jt max-rows))
    jt))


(defn make-ops-impl
  [^DataSource ds opts]
  (let [^JdbcTemplate jt (make-jt ds opts)
        {:keys [clj-to-db db-to-clj]} opts]
    (reify j/Ops
      (batch-update [this sql-coll]         (vec (.batchUpdate jt (into-array String
                                                                              (u/as-list sql-coll)))))
      (batch-update [this sql args-coll]    (vec (Invoke/jtBatchUpdate jt sql
                                                                       (->> (u/as-list args-coll)
                                                                         (map u/as-object-array)
                                                                         doall))))
      (execute        [this sql]        (.execute jt ^String sql))
      (lazy-query     [this f cn? sql]  (.query jt ^String sql
                                          (u/make-resultset-extractor db-to-clj f cn?)))
      (lazy-query     [this f cn? sql args] (.query jt ^String sql (u/as-object-array args)
                                             (u/make-resultset-extractor db-to-clj f cn?)))
      (query-datalist [this sql]        (j/lazy-query this u/realize-resultset false sql))
      (query-datalist [this sql args]   (j/lazy-query this u/realize-resultset false sql args))
      (query-datarow  [this sql]        (u/realize-row (j/query-datalist this sql)))
      (query-datarow  [this sql args]   (u/realize-row (j/query-datalist this sql args)))
      (query-for-list [this sql]        (j/lazy-query this u/realize-resultset true sql))
      (query-for-list [this sql args]   (j/lazy-query this u/realize-resultset true sql args))
      (query-for-row  [this sql]        (u/realize-row (j/query-for-list this sql)))
      (query-for-row  [this sql args]   (u/realize-row (j/query-for-list this sql args)))
      (query-for-val  [this sql]        (.queryForObject jt ^String sql Object))
      (query-for-val  [this sql args]   (.queryForObject jt ^String sql (u/as-object-array args)
                                          Object))
      (update         [this sql]        (.update jt ^String sql))
      (update         [this sql args]   (.update jt ^String sql (u/as-object-array args)))
      (genkey         [this sql]        (u/genkeys #(u/jt-genkey jt sql %)      j/get-key))
      (genkey         [this sql args]   (u/genkeys #(u/jt-genkey jt sql args %) j/get-key))
      (genkey-via     [this f sql]      (u/genkeys #(u/jt-genkey jt sql %)      #(f db-to-clj %)))
      (genkey-via     [this f sql args] (u/genkeys #(u/jt-genkey jt sql args %) #(f db-to-clj %))))))


(defn make-npops-impl
  [^DataSource ds opts]
  (let [^JdbcTemplate jt (make-jt ds opts)
        ^NamedParameterJdbcTemplate njt (NamedParameterJdbcTemplate. jt)
        {:keys [clj-to-db db-to-clj]} opts]
    (reify j/Ops
      (batch-update [this sql-coll]      (vec (.batchUpdate jt (into-array String sql-coll))))
      (batch-update [this sql args-coll] (vec (Invoke/njtBatchUpdate
                                                njt sql (->> (u/as-list args-coll)
                                                          (map #(u/as-string-map clj-to-db %))
                                                          (into-array Map)))))
      (execute        [this sql]        (.execute jt ^String sql))
      (lazy-query     [this f cn? sql]  (.query njt ^String sql (u/make-resultset-extractor
                                                                  db-to-clj f cn?)))
      (lazy-query     [this f cn? sql args] (.query njt ^String sql (u/as-string-map clj-to-db args)
                                              (u/make-resultset-extractor db-to-clj f cn?)))
      (query-datalist [this sql]        (j/lazy-query this u/realize-resultset false sql))
      (query-datalist [this sql args]   (j/lazy-query this u/realize-resultset false sql
                                                      (u/as-string-map clj-to-db args)))
      (query-datarow  [this sql]        (u/realize-row (j/query-datalist this sql)))
      (query-datarow  [this sql args]   (u/realize-row (j/query-datalist this sql args)))
      (query-for-list [this sql]        (j/lazy-query this u/realize-resultset true sql))
      (query-for-list [this sql args]   (j/lazy-query this u/realize-resultset true sql
                                             (u/as-string-map clj-to-db args)))
      (query-for-row  [this sql]        (u/realize-row (j/query-for-list this sql)))
      (query-for-row  [this sql args]   (u/realize-row (j/query-for-list this sql args)))
      (query-for-val  [this sql]        (.queryForObject jt ^String sql Object))
      (query-for-val  [this sql args]   (.queryForObject njt ^String sql
                                          (u/as-string-map clj-to-db args) Object))
      (update         [this sql]        (.update jt ^String sql))
      (update         [this sql args]   (.update njt ^String sql (u/as-string-map clj-to-db args)))
      (genkey         [this sql]        (u/genkeys #(u/jt-genkey jt sql %)
                                                   j/get-key))
      (genkey         [this sql args]   (u/genkeys #(u/njt-genkey clj-to-db njt sql args %)
                                                   j/get-key))
      (genkey-via     [this f sql]      (u/genkeys #(u/jt-genkey jt sql %)
                                                   #(f db-to-clj %)))
      (genkey-via     [this f sql args] (u/genkeys #(u/njt-genkey clj-to-db njt sql args %)
                                                   #(f db-to-clj %))))))


(defn wrap-ops-read-only
  [^Ops ops]
  (let [write-unsupported #(throw (UnsupportedOperationException.
                                    "Only read-only/query operations are supported"))]
    (reify j/Ops
     (batch-update   [this sql-coll]       (write-unsupported))
     (batch-update   [this sql args-coll]  (write-unsupported))
     (execute        [this sql]            (write-unsupported))
     (lazy-query     [this f cn? sql]      (j/lazy-query ops f cn? sql))
     (lazy-query     [this f cn? sql args] (j/lazy-query ops f cn? sql args))
     (query-datalist [this sql]            (j/query-datalist ops sql))
     (query-datalist [this sql args]       (j/query-datalist ops sql args))
     (query-datarow  [this sql]            (j/query-datarow ops sql))
     (query-datarow  [this sql args]       (j/query-datarow ops sql args))
     (query-for-list [this sql]            (j/query-for-list ops sql))
     (query-for-list [this sql args]       (j/query-for-list ops sql args))
     (query-for-row  [this sql]            (j/query-for-row ops sql))
     (query-for-row  [this sql args]       (j/query-for-row ops sql args))
     (query-for-val  [this sql]            (j/query-for-val ops sql))
     (query-for-val  [this sql args]       (j/query-for-val ops sql args))
     (update         [this sql]            (write-unsupported))
     (update         [this sql args]       (write-unsupported))
     (genkey         [this sql]            (write-unsupported))
     (genkey         [this sql args]       (write-unsupported))
     (genkey-via     [this f sql]          (write-unsupported))
     (genkey-via     [this f sql args]     (write-unsupported)))))


(defn wrap-ops-before
  [^Ops ops g]
  (reify j/Ops
    (batch-update   [this sql-coll]       (do (g sql-coll) (j/batch-update ops sql-coll)))
    (batch-update   [this sql args-coll]  (do (g sql) (j/batch-update ops sql args-coll)))
    (execute        [this sql]            (do (g sql) (j/execute ops sql)))
    (lazy-query     [this f cn? sql]      (do (g sql) (j/lazy-query ops f cn? sql)))
    (lazy-query     [this f cn? sql args] (do (g sql) (j/lazy-query ops f cn? sql args)))
    (query-datalist [this sql]            (do (g sql) (j/query-datalist ops sql)))
    (query-datalist [this sql args]       (do (g sql) (j/query-datalist ops sql args)))
    (query-datarow  [this sql]            (do (g sql) (j/query-datarow ops sql)))
    (query-datarow  [this sql args]       (do (g sql) (j/query-datarow ops sql args)))
    (query-for-list [this sql]            (do (g sql) (j/query-for-list ops sql)))
    (query-for-list [this sql args]       (do (g sql) (j/query-for-list ops sql args)))
    (query-for-row  [this sql]            (do (g sql) (j/query-for-row ops sql)))
    (query-for-row  [this sql args]       (do (g sql) (j/query-for-row ops sql args)))
    (query-for-val  [this sql]            (do (g sql) (j/query-for-val ops sql)))
    (query-for-val  [this sql args]       (do (g sql) (j/query-for-val ops sql args)))
    (update         [this sql]            (do (g sql) (j/update ops sql)))
    (update         [this sql args]       (do (g sql) (j/update ops sql args)))
    (genkey         [this sql]            (do (g sql) (j/genkey ops sql)))
    (genkey         [this sql args]       (do (g sql) (j/genkey ops sql args)))
    (genkey-via     [this f sql]          (do (g sql) (j/genkey-via ops f sql)))
    (genkey-via     [this f sql args]     (do (g sql) (j/genkey-via ops f sql args)))))


(defmacro after
  [f sql & body]
  `(let [begin# (System/currentTimeMillis)]
     (try ~@body
       (finally
         (let [elapsed# (- (System/currentTimeMillis) begin#)]
           (~f ~sql elapsed#))))))


(defn wrap-ops-after
  [^Ops ops g]
  (reify j/Ops
    (batch-update   [this sql-coll]       (after g sql-coll (j/batch-update ops sql-coll)))
    (batch-update   [this sql args-coll]  (after g sql (j/batch-update ops sql args-coll)))
    (execute        [this sql]            (after g sql (j/execute ops sql)))
    (lazy-query     [this f cn? sql]      (after g sql (j/lazy-query ops f cn? sql)))
    (lazy-query     [this f cn? sql args] (after g sql (j/lazy-query ops f cn? sql args)))
    (query-datalist [this sql]            (after g sql (j/query-datalist ops sql)))
    (query-datalist [this sql args]       (after g sql (j/query-datalist ops sql args)))
    (query-datarow  [this sql]            (after g sql (j/query-datarow ops sql)))
    (query-datarow  [this sql args]       (after g sql (j/query-datarow ops sql args)))
    (query-for-list [this sql]            (after g sql (j/query-for-list ops sql)))
    (query-for-list [this sql args]       (after g sql (j/query-for-list ops sql args)))
    (query-for-row  [this sql]            (after g sql (j/query-for-row ops sql)))
    (query-for-row  [this sql args]       (after g sql (j/query-for-row ops sql args)))
    (query-for-val  [this sql]            (after g sql (j/query-for-val ops sql)))
    (query-for-val  [this sql args]       (after g sql (j/query-for-val ops sql args)))
    (update         [this sql]            (after g sql (j/update ops sql)))
    (update         [this sql args]       (after g sql (j/update ops sql args)))
    (genkey         [this sql]            (after g sql (j/genkey ops sql)))
    (genkey         [this sql args]       (after g sql (j/genkey ops sql args)))
    (genkey-via     [this f sql]          (after g sql (j/genkey-via ops f sql)))
    (genkey-via     [this f sql args]     (after g sql (j/genkey-via ops f sql args)))))
