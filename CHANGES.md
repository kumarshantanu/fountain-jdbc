# Changes and TODO


* [TODO] Stored procedures (without database metadata, StoredProcedure)
* [TODO] Database metadata assisted Stored Procedures


## 0.3.0 / 2014-Feb-??

* Move project from BitBucket to Github
* Rewrite from scratch, taking essential features from 0.2 release
* Use SpringJDBC 4.0.1
* Protocol based API design (no global state)
* Support :fetch-size and :query-timeout via JdbcTemplate
* Support lazy result-set realization via JdbcTemplate
* Drop support for SimpleJdbcInsert
* Drop dependency Clj-DBSpec, removing global state
* Drop dependency Clj-Miscutil, removing extraneous checks on arguments
* Drop experimental Spring-Batch features


## 0.2 / 2011-Apr-01

* Use Clj-DBSpec 0.2
* Database transactions
* Custom transaction definition
  * Rule based rollback
  * Predicate based rollback
  * Isolation level
  * Propagation behavior
  * Read-only transactions
  * Transaction timeout


## 0.1 / 2011-Mar-06

* Use Spring 3.0.5
* Parameterized SQL statements (SimpleJdbcTemplate)
* Database metadata assisted Inserts (SimpleJdbcInsert)

