(ns runtests
  (:require
    [org.bituf.fountain.test-jdbc        :as jd]
    [org.bituf.fountain.test-transaction :as tx]
    [org.bituf.fountain.test-batch       :as ba])
  (:use clojure.test))


;; enable logging
;(org.apache.log4j.BasicConfigurator/configure)


(run-tests
  'org.bituf.fountain.test-jdbc
  'org.bituf.fountain.test-transaction
  'org.bituf.fountain.test-batch)
