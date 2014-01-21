(ns daedal.git
  "Implementation of git repo"
  (:require [clojure.tools.logging :as log])
  (:import [java.io
            ByteArrayInputStream
            InputStream]
           [java.util Arrays]
           [java.util.concurrent ConcurrentHashMap]
           [org.eclipse.jgit.lib
            AbbreviatedObjectId
            AnyObjectId
            Constants
            ObjectDatabase
            ObjectId
            ObjectInserter
            ObjectLoader
            ObjectReader
            PersonIdent
            Ref
            RefDatabase
            RefUpdate
            Repository
            RepositoryBuilder
            StoredConfig]
           [org.eclipse.jgit.revwalk RevCommit]
           [org.eclipse.jgit.transport
            PackParser]))


(defn not-implemented
  []
  (throw (ex-info "Not yet implemeted"
                  {:reason :not-implemented})))

;; gen-class is another way to do it. This makes interactive
;; development a bit weird, though.

;; (gen-class :name daedal.git.MemObjectInserter
;;            :state state
;;            :init "init"
;;            :constructors {[Object] []}
;;            :prefix "mem-inserter-")

;; (defn mem-inserter-init
;;   [db]
;;   [[] db])

;; (defn mem-inserter-insert
;;   [type data]
;;   (not-implemented))

(defprotocol ObjectInfo
  (get-info [this]))

(extend-protocol ObjectInfo
  nil
  (get-info [_] nil)

  RevCommit
  (get-info [commit]
    {:message (.getFullMessage commit)
     :author (-> commit .getAuthorIdent get-info)
     :committer (-> commit .getCommitterIdent get-info)
     :commit-time (-> commit .getCommitTime)})

  PersonIdent
  (get-info [ident]
    {:name (-> ident .getName)
     :email (-> ident .getEmailAddress)}))

(defprotocol Bytes
  (get-bytes [this off len]))

(extend-protocol Bytes
  (class (byte-array 0))
  (get-bytes [this off len]
    (if off
      (Arrays/copyOfRange ^bytes this ^int off ^int (+ off len))
      this))

  String
  (get-bytes [this off len]
    (when (or off len)
      (not-implemented))
    (.getBytes this (java.nio.charset.Charset/forName "UTF-8")))

  ByteArrayInputStream
  (get-bytes [this off len]
    (let [off (or off 0)
          len (or len (.available this))
          buf (byte-array len)]
      (.read this buf off len)
      buf)))

(def type-name
  {Constants/OBJ_COMMIT :commit
   Constants/OBJ_TREE   :tree
   Constants/OBJ_BLOB   :blob})

(def reverse-type-map
  {:commit Constants/OBJ_COMMIT
   :tree   Constants/OBJ_TREE
   :blob   Constants/OBJ_BLOB})

(defn parse-info
  [type bits]
  (case (type-name type)
    :commit (-> bits RevCommit/parse get-info)
    :tree   {}
    :blob   {}))

(defn mem-insert-obj
  "Inserts an object into the database `db`, returning its new ID."
  [^ObjectInserter inserter db type data & [off len]]
  (let [bits (get-bytes data off len)
        id (.idFor inserter type bits)]
    (-> db
        :objects
        (swap! assoc
               (.name id)
               {:id        (.name id)
                :type      (type-name type)
                :data      bits
                :data-type (class data)
                :info      (parse-info type bits)}))
    id))

(defn mem-pack-parser
  [db object-database in]
  (let [state (atom {})
        crc (java.util.zip.CRC32.)]
    (proxy [PackParser] [object-database in]
      (onAppendBase [type-code data info]
        (not-implemented))
      (onBeginOfsDelta [delta-stream-position base-stream-position inflated-size]
        (not-implemented))
      (onBeginRefDelta [delta-stream-position base-id inflated-size]
        (not-implemented))
      (onBeginWholeObject [stream-position type inflated-size]
        (.reset crc))
      (onEndThinPack []
        (not-implemented))
      (onEndWholeObject [info]
        (not-implemented))
      (onInflatedObjectData [obj type-code data]
        (not-implemented))
      (onObjectData [src raw pos len]
        (.update crc raw pos len))
      (onObjectHeader [src raw pos len]
        (.update crc raw pos len))
      (onPackFooter [hash]
        (not-implemented))
      (onPackHeader [obj-cnt]
        (swap! state assoc :object-count obj-cnt))
      (onStoreStream [raw pos len]
        (not-implemented))
      (readDatabase [dst pos cnt]
        (not-implemented))
      (seekDatabase [obj info]
        ;; Note that there are two two-arity overloads of this method.
        ;; Need to resolve by type in the body.
        (not-implemented)))))

(defn mem-object-inserter
  [object-database storage]
  (proxy [ObjectInserter] []
    (flush []
      ;; TODO: We could have some sort of pending/active split in the
      ;; DB, but I'm not sure I see much point in a prototype. For
      ;; now, all writes take effect immediately
      )
    (insert
      ;; This is sort of icky, but it doesn't look like Clojure will
      ;; correctly resolve the arity-two call to the base class.
      ;; Instead, it resolves the call to the arity-three proxy
      ;; method, which it then calls with the incorrect number of
      ;; arguments. So far this is the only way around this I've
      ;; found: to implement and explitly forward the call.
      ([type ^bytes data] (mem-insert-obj this storage type data))
      ([type len in] (mem-insert-obj this storage type in))
      ([type data off len] (mem-insert-obj this storage type data off len)))
    (newPackParser [^InputStream in] (mem-pack-parser storage object-database in))
    (release [] ; no-op
      )))

(defn mem-object-loader
  [storage ^AnyObjectId object-id]
  (let [obj (-> @storage :objects (get (.name object-id)))]
    (proxy [ObjectLoader] []
      (getType [] (-> obj :type reverse-type-map))
      (getCachedBytes
        ([] (:data obj))
        ([size-limit] (if obj
                        (:data obj)
                        (do
                          (log/warn "Couldn't find object"
                                    :object-id object-id)
                          (throw (org.eclipse.jgit.errors.MissingObjectException.
                                  (.copy object-id)
                                  "unknown"))))))
      (openStream [] (-> obj :data ByteArrayInputStream.)))))

(defn mem-object-reader
  [storage]
  (proxy [ObjectReader] []
    (getShallowCommits []
      ;; I'm not sure what shallow commits are, but it looks like they
      ;; can be turned off.
      #{})
    (newReader [] (mem-object-reader storage))
    (open
      ([^AnyObjectId object-id]
         (mem-object-loader storage object-id))
      ([^AnyObjectId object-id type-int]
         (.open ^ObjectReader this object-id)))
    (resolve [^AbbreviatedObjectId id] (not-implemented))))

(defn mem-object-database
  [storage]
  (proxy [ObjectDatabase] []
    (close [])                          ; No-op
    (newInserter [] (mem-object-inserter this storage))
    (newReader [] (mem-object-reader storage))))

(defn mem-ref
  [r]
  (proxy [Ref] []
    (getLeaf [] this)
    (getName [] (:name r))
    (getObjectId [] (when-let [id (:id r)] (ObjectId/fromString id)))
    (getPeeledObjectId [] (not-implemented))
    (getStorage [] (not-implemented))
    (getTarget [] (not-implemented))
    (isPeeled [] (not-implemented))
    (isSymbolic [] (not-implemented))))

(defn mem-ref-update
  [^RefDatabase ref-db db repo ref-name]
  (proxy [RefUpdate] [(or (.getRef ref-db ref-name)
                          (mem-ref {:name ref-name}))]
    (doDelete [desired-result] (not-implemented))
    (doLink [target] (not-implemented))
    (doUpdate [desired-result]
      (let [refs ^ConcurrentHashMap (:refs db)]
        (-> refs (.put ref-name {:name ref-name
                                 :id (-> ^RefUpdate this .getNewObjectId .name)})))
      desired-result)
    (getRefDatabase [] ref-db)
    (getRepository [] repo)
    (tryLock [deref?]
      ;; TODO: Always succeeds for now
      true
      )
    (unlock []
      ;; TODO: no-op for now
      ;;(not-implemented)
      )))

(defn get-refs
  [storage db repo ^String prefix]
  (or (empty? prefix)
      (.endsWith prefix "/")
      (throw (ex-info "Invalid prefix: must be empty or end with slash"
                      {:reason :invalid-prefix
                       :prefix prefix})))
  (->> db
       :refs
       (filter (fn [[^String n r]] (.startsWith n prefix)))
       (map (fn [[n r]] [(subs n (.length prefix)) r]))
       (into {})
       ConcurrentHashMap.))

(defn mem-ref-database
  [storage db repo]
  (proxy [RefDatabase] []
    (close [])                          ; No-op
    (create [] (not-implemented))
    (getAdditionalRefs [] (not-implemented))
    (getRef [name] (some-> @storage :refs (get name) mem-ref))
    (getRefs [prefix] (get-refs storage db repo prefix))
    (isNameConflicting [name]
      ;; TODO: This isn't right - we have to disallow
      ;; refs/heads/master/blah if refs/heads/master already exists,
      ;; and vice-versa.
      (let [refs ^ConcurrentHashMap (:refs @storage)]
        (.containsKey refs name)))
    (newRename [from-name to-name] (not-implemented))
    (newUpdate [name detach?]
      (when detach? (not-implemented))
      (mem-ref-update this db repo name))
    (peel [ref] (not-implemented))))

(defn mem-repo-builder
  []
  (proxy [RepositoryBuilder] []))

(defn mem-stored-config
  []
  (proxy [StoredConfig] []
    (load [] (not-implemented))
    (save [] (not-implemented))
    ;; TODO: Automate this wrapping - this is sort of tedious.
    (getBoolean
      ([section name default-value]
         (let [result (proxy-super getBoolean section name default-value)]
           (log/debug "StoredConfig.getBoolean"
                      :section section
                      :name name
                      :default-value default-value
                      :result result)
           (if (= ["http" "receivepack"] [section name])
             (do
               (log/warn "Returning hardcoded true for http.receivepack")
               true)
             result)))
      ([section subsection name default-value]
         (let [result (proxy-super getBoolean section subsection name default-value)]
           (log/debug "StoredConfig.getBoolean"
                      :section section
                      :subsection subsection
                      :name name
                      :default-value default-value
                      :result result)
           result)))))

(defn ^Repository mem-repo
  [uri repo-name]
  (let [conn (not-implemented)
        db   (mem-object-database conn)]
    (proxy [Repository] [(mem-repo-builder)]
      (create [bare?] (not-implemented))
      (getConfig [] (not-implemented))
      (getObjectDatabase [] db)
      (getRefDatabase [] (not-implemented))
      (getReflogReader [^String ref-name] (not-implemented))
      (notifyIndexChanged [] (not-implemented))
      (scanForRepoChanges [] (not-implemented)))))
