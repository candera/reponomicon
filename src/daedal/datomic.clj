(ns daedal.datomic
  "Functions for working with Datomic."
  (:require [daedal.common :as com]
            [datomic.api :as d :refer (q conn)]))

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
  "Transacts `schema` through `conn`, but only if it hasn't already
  been transacted into the database."
  [conn schema]
  (let [schemas-str (pr-str schema)
        schema-id (str (com/digest schemas-str) schema-key)]
    (if (schema-present? (d/db conn) schema-id)
      (log/info "Schema"
                schema-id
                "is already present in the database. Doing nothing.")
      (do
        (log/info "Asserting schema" schema-id)
        (transact-schema conn
                         (-> schemas-str read-string (get schema-key))
                         schema-id
                         inst)))))
