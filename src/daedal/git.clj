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
           [org.eclipse.jgit.revwalk RevCommit]))


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

(defn mem-object-inserter
  [db]
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
      ([type ^bytes data] (mem-insert-obj this db type data))
      ([type len in] (mem-insert-obj this db type in))
      ([type data off len] (mem-insert-obj this db type data off len)))
    (newPackParser [^InputStream in] (not-implemented))
    (release [] (not-implemented))))

(defn mem-object-loader
  [obj]
  (proxy [ObjectLoader] []
    (getType [] (-> obj :type reverse-type-map))
    (getCachedBytes
      ([] (:data obj))
      ([size-limit] (:data obj)))
    (openStream [] (-> obj :data ByteArrayInputStream.))))

(defn mem-object-reader
  [data]
  (proxy [ObjectReader] []
    (getShallowCommits []
      ;; I'm not sure what shallow commits are, but it looks like they
      ;; can be turned off.
      #{})
    (newReader [] (mem-object-reader data))
    (open
      ([^AnyObjectId object-id]
         (-> data :objects deref (get (.name object-id)) mem-object-loader))
      ([^AnyObjectId object-id type-int]
         (.open ^ObjectReader this object-id)))
    (resolve [^AbbreviatedObjectId id] (not-implemented))))

(defn mem-object-database
  [data]
  (proxy [ObjectDatabase] []
    (close [])                          ; No-op
    (newInserter [] (mem-object-inserter data))
    (newReader [] (mem-object-reader data))))

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
  [db repo ^String prefix]
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
  [db repo]
  (proxy [RefDatabase] []
    (close [])                          ; No-op
    (create [] (not-implemented))
    (getAdditionalRefs [] (not-implemented))
    (getRef [name] (some-> db :refs (get name) mem-ref))
    (getRefs [prefix] (get-refs db repo prefix))
    (isNameConflicting [name]
      ;; TODO: This isn't right - we have to disallow
      ;; refs/heads/master/blah if refs/heads/master already exists,
      ;; and vice-versa.
      (let [refs ^ConcurrentHashMap (:refs db)]
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
    (save [] (not-implemented))))

(defn ^Repository mem-repo
  []
  (let [db     {:refs    (ConcurrentHashMap.)  ; Maps ref name to ref object
                :objects (atom {})
                :config  (mem-stored-config)}
        obj-db (mem-object-database db)]
    [(proxy [Repository] [(mem-repo-builder)]
       (create [bare?] (not-implemented))
       (getConfig [] (:config db))
       (getObjectDatabase [] obj-db)
       (getRefDatabase [] (mem-ref-database db this))
       (getReflogReader [^String ref-name] (not-implemented))
       (notifyIndexChanged [] (not-implemented))
       (scanForRepoChanges [] (not-implemented)))
     db]))
