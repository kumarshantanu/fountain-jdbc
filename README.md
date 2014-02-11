# fountain-jdbc

Fountain-JDBC is a Clojure wrapper for SpringJDBC (a component to deal with
JDBC in [Spring framework](http://projects.spring.io/spring-framework/).)
Currently the following are supported:

* Parameterized SQL statements
* Transactions
   1. programmatic
   2. predicate based
   3. rule based


## Usage

This is a rewrite of the previous versions on
[Bitbucket repo](https://bitbucket.org/kumarshantanu/fountain-jdbc/overview).
It is not on Clojars yet.

Examples for usage can be found in the _Quicksart_ section below:


## Building/Installation

You will need Leiningen 2 or higher to build from sources. Execute the following:

```bash
$ lein do clean, dev test # run tests once
$ lein do clean, all test # run tests on all supported Clojure versions
$ lein do clean, install  # install artifact locally
```


## Documentation

You can include Fountain-JDBC to your project by including its namespace:

```clojure
(require '[fountain.jdbc :as j])
(require '[fountain.jdbc.factory :as f])
```


### Quickstart

Create a new project and include the following dependencies:


```clojure
[clj-dbcp "0.8.1"]
[fountain-jdbc "0.3.0-SNAPSHOT"]
[com.h2database/h2 "1.3.175"]  ; H2 driver, or any JDBC driver you like
```

Define the DDL and CRUD functions as follows:

```clojure
(ns fooapp.core
  (:require [fountain.jdbc         :as j]
            [fountain.jdbc.factory :as f]
            [clj-dbcp.core         :as cp]))

;; configuration

(def dbspec (f/assoc-txn-manager
              {:datasource (cp/make-datasource
                             :h2 {:target :memory :database "emp"
                                  :user "sa" :password ""})}))

(def ops (f/make-ops-impl dbspec))

;; operations

(def ddl-ct "CREATE TABLE sample
            (sample_id INT         NOT NULL PRIMARY KEY AUTO_INCREMENT,
             name      VARCHAR(30) NOT NULL,
             age       INT)")

(defn demo
  []
  ;; create table
  (j/execute ops ddl-ct)
  ;; insert rows (returns the generated key)
  (j/genkey ops "INSERT INTO sample (name, age) VALUES (?, ?)" ["Harry" 30])
  (j/genkey ops "INSERT INTO sample (name, age) VALUES (?, ?)" ["Megan" 40])
  ;; retrieve
  (j/query-for-val ops "SELECT name FROM sample WHERE age = ?" [30])
  (j/query-for-row ops "SELECT sample_id, name FROM sample WHERE age = ?" [30])
  (j/query-for-list ops "SELECT * FROM sample")
  ;; update
  (j/update ops "UPDATE sample SET age=? WHERE name=?" [35 "Harry"])
  ;; delete
  (j/update ops "DELETE FROM sample where age=?" [40]))
```

### Reference

TBD


## License

Copyright © 2011,2014 Shantanu Kumar

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
