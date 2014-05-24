(ns reponomicon.git.object-storage.datomic
  "Stores git objects in Datomic"
  (:require [datomic.api :as d]
            [reponomicon.git.object-storage :as storage]))

;; Obviously, the Datomic store should only be used for very small
;; objects. Specifically, we use it for the config repo, since we need
;; a place to store configuration data in a repo that doesn't itself
;; require configuration like what directory to use.
(defrecord DatomicObjectStore [conn]
  storage/ObjectStorage
  (obj-stream [store obj-name]
    (-> (d/entity (d/db conn) [:object/sha obj-name])
        :object/bytes
        java.io.ByteArrayInputStream.))

  (write-obj [store obj-name data]
    (let [bytes (com.google.common.io.ByteStreams/toByteArray data)]
      @(d/transact conn
                   [{:db/id (d/tempid :part/trees-and-blobs)
                     :object/sha obj-name
                     :object/bytes bytes}]))))

(defn create-store
  "Returns an instance of `ObjectStorage` over Datomic."
  [conn]
  (map->DatomicObjectStore {:conn conn}))
