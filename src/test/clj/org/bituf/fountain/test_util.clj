(ns org.bituf.fountain.test-util
  (:import
    (java.sql Connection Statement ResultSet))
  (:require
    [org.bituf.clj-miscutil  :as mu]
    [org.bituf.clj-dbspec    :as sp]
    [org.bituf.clj-liquibase :as lb]
    [org.bituf.clj-liquibase.change :as ch])
  (:use clojure.test))


(defmacro is-thrown?
  "Check if given exception is thrown in a test. `ex` can be an exception class
  or an exception object."
  [ex & body]
  `(is (instance? (if (class? ~ex) ~ex
                    (class ~ex))
         (last (mu/maybe ~@body)))))


(defmacro with-liquibase
  [& body]
  `(let [g# (lb/wrap-lb-init (fn [] ~@body))]
     (g#)))


(defmacro with-stmt
  [st & body]
  `((sp/wrap-connection
      (fn []
        (with-open [~st (.createStatement ^Connection (:connection sp/*dbspec*))]
          (do ~@body))))))


(defn row-count
  [table-name]
  (with-stmt ^Statement st
    (let [^ResultSet rs (.executeQuery st (str "SELECT COUNT(*) FROM "
                                            (sp/db-iden table-name)))]
      (.next rs)
      (.getInt rs 1))))


(defn sample-setup
  []
  (let [sql-coll (with-liquibase
                   (lb/change-sql (ch/create-table-withid :sample
                                    [[:name [:varchar 30] :null false]
                                     [:age  :int]])))]
    (with-stmt ^Statement st
      (mu/maybe (.execute st "DROP TABLE sample"))
      (doseq [each sql-coll]
        (println "\n" each "\n")
        (.execute st ^String each))
      (.execute st "INSERT INTO sample (name, age)
                    VALUES ('Harry', 30)"))))


(defn sample-count
  []
  (row-count :sample))


(defn sample2-setup
  []
  (let [sql-coll (with-liquibase
                   (lb/change-sql (ch/create-table-withid :sample2
                                    [[:extra-id :int          :null false :autoinc true]
                                     [:name     [:varchar 30] :null false]
                                     [:age      :int]])))]
    (with-stmt ^Statement st
      (mu/maybe (.execute st "DROP TABLE sample2"))
      ;; Liquibase creates wrong CREATE TABLE SQL for 1+ auto-inc columns
      ;(doseq [each sql-coll]
      ;  (println "\n\n**** Executing Liquibase SQL: " each "\n")
      ;  (.execute st ^String each))
      (.execute st "CREATE TABLE sample2
                     (sample_id INT         NOT NULL PRIMARY KEY AUTO_INCREMENT,
                      extra_id  INT         NOT NULL AUTO_INCREMENT,
                      name      VARCHAR(30) NOT NULL,
                      age       INT)")
      (.execute st "INSERT INTO sample2 (name, age)
                    VALUES ('Harry', 30)"))))


(defn sample2-count
  []
  (row-count :sample2))


(defn fail
  ([msg] (is false msg))
  ([] (is false)))


