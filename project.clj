(defproject fountain-jdbc "0.3.0-SNAPSHOT"
  :description "Clojure wrapper for SpringJDBC"
  :url "https://github.com/kumarshantanu/fountain-jdbc"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :mailing-list {:name "Bitumen Framework discussion group"
                 :archive "https://groups.google.com/group/bitumenframework"
                 :other-archives ["https://groups.google.com/group/clojure"]
                 :post "bitumenframework@googlegroups.com"}
  :java-source-paths ["java-src"]
  :javac-options {:destdir "target/classes/"
                  :source  "1.5"
                  :target  "1.5"}
  :dependencies [[org.springframework/spring-jdbc "4.0.1.RELEASE"]]
  :profiles {:dev {:dependencies [[oss-jdbc "0.8.0"]
                                  [clj-dbcp "0.8.1"]
                                  [net.mikera/cljunit "0.3.0"]]}
             :1.4 {:dependencies [[org.clojure/clojure "1.4.0"]]}
             :1.5 {:dependencies [[org.clojure/clojure "1.5.1"]]}
             :1.6 {:dependencies [[org.clojure/clojure "1.6.0-beta2"]]}}
  :aliases {"dev" ["with-profile" "dev,1.5"]
            "all" ["with-profile" "dev,1.4:dev,1.5:dev,1.6"]}
  :global-vars {*warn-on-reflection* true}
  :min-lein-version "2.0.0")
