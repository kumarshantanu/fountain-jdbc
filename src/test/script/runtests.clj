(ns runtests
  (:use org.bituf.fountain.test-jdbc)
  (:use clojure.test))


(run-tests
  'org.bituf.fountain.test-jdbc)
