(ns fountain.jdbc.test-util
  (:require [fountain.jdbc         :as j]
            [fountain.jdbc.factory :as f]
            [clojure.test :refer :all]))


(defmacro is-thrown?
  "Check if given exception is thrown in a test. `ex` can be an exception class
  or an exception object."
  [ex & body]
  `(is (instance? (if (class? ~ex) ~ex
                    (class ~ex))
                  (try ~@body nil
                    (catch Throwable e#
                      e#)))))


(def sample-ddl ["CREATE TABLE sample (
  id INT AUTO_INCREMENT PRIMARY KEY,
  name VARCHAR(30) NOT NULL, age INT, gender CHAR(1), is_emp BOOLEAN)"])


(defn sample-setup
  [dbspec]
  (let [ops (f/make-ops-impl (dissoc dbspec :read-only?))]
    (try (j/execute ops "DROP TABLE sample")
      (catch RuntimeException e (println "Ignoring table drop exception")))
    (doseq [each sample-ddl]
      (j/execute ops each))
    (j/execute ops "INSERT INTO sample (name, age, gender, is_emp) VALUES ('Harry', 30, 'M', true)")
    (j/execute ops "INSERT INTO sample (name, age, gender, is_emp) VALUES ('Sally', 40, 'F', true)")))


(defmacro with-sample
  [dbspec ops npops & body]
  `(do
     (tu/sample-setup ~dbspec)
     (let [~ops   (f/make-ops-impl ~dbspec)
           ~npops (f/make-ops-impl (assoc ~dbspec :named-params? true))]
       ~@body)))


(defn sample-count
  [ops]
  (j/query-for-val ops "SELECT COUNT(*) FROM sample"))


(def sample2-ddl ["CREATE TABLE sample2 (
  id INT AUTO_INCREMENT PRIMARY KEY, aux_id INT AUTO_INCREMENT, name VARCHAR(30) NOT NULL)"])


(defn sample2-setup
  [dbspec]
  (let [ops (f/make-ops-impl dbspec)]
    (try (j/execute ops "DROP TABLE sample2")
      (catch RuntimeException e (println "Ignoring table drop exception")))
    (doseq [each sample2-ddl]
      (j/execute ops each))
    (j/execute ops "INSERT INTO sample2 (name) VALUES ('Megha')")))