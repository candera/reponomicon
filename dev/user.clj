(ns user
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.java.javadoc :refer (javadoc)]
            [clojure.pprint :as pprint :refer (pprint)]
            [clojure.repl :refer :all]
            [clojure.tools.logging :as log]
            [clojure.tools.namespace.repl :refer (refresh refresh-all)]
            [clojure.tools.trace :refer (trace-ns)]
            [com.stuartsierra.component :as component]
            [datomic.api :as d]
            [gitomic.common :as com]
            [gitomic.datomic :as datomic]
            [gitomic.datomic.schema :as schema]
            [gitomic.git :refer :all :as git]
            [gitomic.system-instance :as system-instance])
  (:refer-clojure :exclude [methods])
  (:import [java.io ByteArrayInputStream]
           [org.eclipse.jetty.server Server]
           [org.eclipse.jetty.servlet ServletContextHandler ServletHolder]
           [org.eclipse.jgit.http.server GitServlet]
           [org.eclipse.jgit.lib
            AbbreviatedObjectId
            AnyObjectId
            CommitBuilder
            Constants
            FileMode
            ObjectDatabase
            ObjectId
            ObjectInserter
            ObjectReader
            PersonIdent
            Ref
            RefDatabase
            RefUpdate
            Repository
            RepositoryBuilder
            TreeFormatter]
           [org.eclipse.jgit.transport.resolver RepositoryResolver UploadPackFactory]
           [org.eclipse.jgit.transport ReceivePack UploadPack]))

;;; Development-time components

(defn create-jetty-server
  [^long port datomic]
  (let [server        (Server. port)
        repo-resolver (reify org.eclipse.jgit.transport.resolver.RepositoryResolver
                        (open [this req name]
                          (log/debug {:method :repository-resolver/open
                                      :name name})
                          (make-repo name datomic)))
        upack-factory (proxy [org.eclipse.jgit.transport.resolver.UploadPackFactory] []
                        (create [req repo]
                          (log/debug {:method :upload-pack-factory/create
                                      :request req
                                      :repo repo})
                          (UploadPack. repo)))
        rpack-factory (proxy [org.eclipse.jgit.transport.resolver.ReceivePackFactory] []
                        (create [req repo]
                          (log/debug {:method :receive-pack-factory/create
                                      :request req
                                      :repo repo})
                          (ReceivePack. repo)))
        servlet       (doto (GitServlet.)
                        (.setRepositoryResolver repo-resolver)
                        (.setUploadPackFactory upack-factory)
                        (.setReceivePackFactory rpack-factory))
        context       (doto (ServletContextHandler.)
                        (.setContextPath "/")
                        (.addServlet (ServletHolder. servlet) "/*"))]
    (.setHandler server context)
    server))

(defn jetty-server
  "Returns a Lifecycle wrapper around embedded Jetty for development."
  [port datomic]
  (let [server (atom nil)]
    (reify component/Lifecycle
      (start [_]
        (when-not @server
          (log/info :STARTING "jetty" :port port)
          (reset! server
                  (create-jetty-server port datomic))
          (.start ^Server @server)
          (log/info :STARTED "jetty" :port port)))
      (stop [_]
        (when @server
          (log/info :STOPPING "jetty" :port port)
          (.stop ^Server @server)
          (reset! server nil)
          (log/info :STOPPED "jetty" :port port))))))

(def dev-system-components [:jetty :datomic])

;; Do not create directly; use dev-system function
;; options is only here so we can look at it later if we want to.
(defrecord DevSystem [jetty options datomic]
  component/Lifecycle
  (start [this]
    (component/start-system this dev-system-components))
  (stop [this]
    (component/stop-system this dev-system-components)))

(defn create-repo
  "Creates an empty repo named `name` in the database."
  [conn repo-name repo-description]
  @(d/transact conn [{:db/id            (d/tempid :part/repos)
                      :repo/name        repo-name
                      :repo/description repo-description}]))

(defn dev-system
  "Returns a complete system in :dev mode for development at the REPL.
  Options are key-value pairs from:

      :port        Web server port, default is 9900"
  [& {:keys [port]
      :or {port 9900}
      :as options}]
  (let [schema  schema/schema
        datomic (datomic/temp-peer (:txes schema) (:id schema))
        jetty   (jetty-server port datomic)]
    ;; TODO: If we start to have dependencies, make use of component/using
    (map->DevSystem {:jetty   jetty
                     :datomic datomic
                     :options options})))

;;; Access to dev-time datomic database

(defn conn
  []
  "A connection to the dev database"
  (-> system-instance/system :datomic :uri d/connect))

(defn db
  "The latest available value of the dev database"
  []
  (d/db (conn)))

;;; Development system lifecycle

(defn init
  "Initializes the development system."
  [& options]
  (alter-var-root #'system-instance/system (constantly (apply dev-system options))))

(defn system
  "Returns the system instance"
  []
  system-instance/system)

;; If desired change this to a vector of options that will get passed
;; to dev-system on (reset)
(def default-options [])

(defn go
  "Launches the development system. Ensure it is initialized first."
  [& options]
  (let [options (or options default-options)]
    (when-not system-instance/system (apply init options)))
  (component/start system-instance/system)
  (log/debug "go"
             :uri (-> system-instance/system :datomic :uri))
  (create-repo (conn) "bar" "Test repo")
  (set! *print-length* 20)
  :started)

(defn stop
  "Shuts down the development system and destroy all its state."
  []
  (when system-instance/system
    (component/stop system-instance/system)
    (alter-var-root #'system-instance/system (constantly nil)))
  :stopped)

(defn reset
  "Stops the currently-running system, reload any code that has changed,
  and restart the system."
  []
  (stop)
  (refresh :after 'user/go))

(defn methods
  [^Class c]
  (->> c .getMethods (map str) (into [])))

(defn slurp-bytes
  [path]
  (let [f (io/file path)
        len (.length f)
        barr (byte-array len)]
    (-> f
        java.io.FileInputStream.
        java.io.DataInputStream.
        (.readFully barr))
    barr))
