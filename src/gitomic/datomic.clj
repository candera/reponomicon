(ns gitomic.datomic
  "Functions for working with Datomic."
  (:require [clojure.string :as str]
            [clojure.tools.logging :as log]
            [com.stuartsierra.component :as component]
            [gitomic.common :as com]
            [datomic.api :as d :refer (q connect)]))


(defn- schema-present?
  "Returns true if the schema identified by `id` has already been
  transacted."
  [db id]
  (and (d/entid db :schema/transacted-schema-id)
       (seq (q '[:find ?e
                 :in $ ?id
                 :where [?e :schema/transacted-schema-id ?id]]
               db
               id))))

(defn- transact-schema
  "Transacts transaction data `schema` and records the fact by adding
  `schema-id` to the list of transacted schemas. If `inst` is non-nil,
  adds a `:db/txInstant` assertion to all transactions."
  [conn schema schema-id inst]
  (let [inst-assertion {:db/txInstant inst
                        :db/id (d/tempid :db.part/tx)}]
    (doseq [tx schema]
      @(d/transact conn
                   (if inst (conj tx inst-assertion) tx)))
    (let [schema-assertion {:db/id (d/tempid :part/schemas)
                            :schema/transacted-schema-id schema-id}]
      @(d/transact conn
                   (if inst
                     [schema-assertion inst-assertion]
                     [schema-assertion])))))

(defn ensure-schema
  "Transacts the application schema identified by `schema-key` through
  `conn`, but only if it hasn't already been transacted into the
  database. If `inst` is specified, sets the time of the transaction
  to it via a :db/txInstant assertion."
  ([conn schema schema-id] (ensure-schema conn schema schema-id nil))
  ([conn schema schema-id inst]
     (if (schema-present? (d/db conn) schema-id)
       (log/info "Schema"
                 schema-id
                 "is already present in the database. Doing nothing.")
       (do
         (log/info "Asserting schema" schema-id)
         (transact-schema conn
                          schema
                          schema-id
                          inst)))))

(defn init-db
  "Ensures the database schema is asserted. conn is a Datomic
  connection. If `inst` is specified, transacts it in the past."
  ([conn schema schema-id] (init-db conn schema schema-id nil))
  ([conn schema schema-id inst]
     (ensure-schema conn schema schema-id inst)))

;; Do not create directly; use temp-peer function
(defrecord TemporaryPeer [uri schema schema-id]
  component/Lifecycle
  (start [_]
    (log/info :STARTING "temporary-peer" :uri uri)
    (d/create-database uri)
    (let [conn (d/connect uri)]
      (init-db conn schema schema-id)))
  (stop [_]
    (log/info :STOPPING "temporary-peer" :uri uri)
    (d/delete-database uri)))

(defn temp-peer
  "Returns an object implementing the Lifecycle protocol for a
  temporary, in-memory Datomic database. Suitable for development and
  testing. Creates a uniquely-named database on startup, asserts the
  schema and sample data. Deletes the database on shutdown. The
  Datomic database URI is available as the key :uri."
  [schema schema-id]
  (let [name (d/squuid)
        uri (str "datomic:mem:" name)]
    (map->TemporaryPeer {:uri       uri
                         :schema    schema
                         :schema-id schema-id})))

;; Do not create directly; use persistent-peer function
(defrecord PersistentPeer [uri
                           schema
                           schema-id
                           memcached-nodes]
  component/Lifecycle
  (start [_]
    (log/info :STARTING "persistent-peer" :uri uri :memcached-nodes memcached-nodes)
    (when-not (str/blank? memcached-nodes)
      (System/setProperty "datomic.memcacheServers" memcached-nodes))
    (try
      (let [conn (d/connect uri)]
        (init-db conn schema schema-id))
      (catch Throwable t
        (log/error t :STARTING "Failed to initialize database")
        (throw t))))
  (stop [_]
    (log/info :STOPPING "persistent-peer" :uri uri)))

(defn persistent-peer
  "Returns an object implementing the Lifecycle protocol for a
  persistent Datomic database using the given URI and memcached nodes
  (which may be blank). Ensures on startup that the database has been
  created and the schema has been asserted."
  ([uri schema schema-id]
     (persistent-peer uri schema schema-id ""))
  ([uri schema schema-id memcached-nodes]
     (map->PersistentPeer {:uri             uri
                           :schema          schema
                           :schema-id       schema-id
                           :memcached-nodes memcached-nodes})))
