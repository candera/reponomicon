(defproject daedal "0.0.1-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.6.0"]
                 [org.clojure/tools.logging "0.2.6"]

                 [com.datomic/datomic-free "0.9.4766"
                  :exclusions [org.slf4j/slf4j-nop
                               org.slf4j/slf4j-log4j12]]

                 [com.google.guava/guava "17.0"]

                 [com.stuartsierra/component "0.2.1"]

                 [ch.qos.logback/logback-classic "1.1.2"]

                 [org.eclipse.jgit/org.eclipse.jgit "3.2.0.201312181205-r"]
                 [org.eclipse.jgit/org.eclipse.jgit.http.server "3.2.0.201312181205-r"]

                 [org.eclipse.jetty/jetty-server "9.2.0.M0"]
                 [org.eclipse.jetty/jetty-servlet "9.2.0.M0"]
                 [javax.servlet/javax.servlet-api "3.1.0"]

                 [org.apache.sshd/sshd-core "0.11.0"]

                 ;; [org.slf4j/jcl-over-slf4j "1.7.2"]
                 ;; [org.slf4j/slf4j-api "1.7.2"]
                 ;; [org.slf4j/slf4j-log4j12 "1.6.4"]
                 ;; [log4j/log4j "1.2.16"]
                 ]
  :global-vars {*warn-on-reflection* true}
  :min-lein-version "2.0.0"
  :resource-paths ["resources"]
  :main ^{:skip-aot true} daedal.main
  :profiles
  {:dev {:dependencies [[org.clojure/tools.namespace "0.2.4"]
                        [org.clojure/tools.trace "0.7.8"]]
         :jvm-opts     ["-Xdebug"
                        "-Xrunjdwp:transport=dt_socket,server=y,suspend=n,address=9901"]
         :repl-options {:init-ns user}
         :source-paths ["dev"]}})
