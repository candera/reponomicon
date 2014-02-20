(ns daedal.git
  "Implementation of git repo"
  (:require [clojure.java.io :as io]
            [clojure.tools.logging :as log]
            [datomic.api :as d])
  (:import [com.google.common.io
            Files]
           [java.io
            ByteArrayInputStream
            File
            InputStream]
           [java.nio ByteBuffer]
           [java.util Arrays]
           [java.util.concurrent ConcurrentHashMap]
           [org.eclipse.jgit.errors MissingObjectException]
           [org.eclipse.jgit.internal.storage.file
            PackIndexWriter
            PackFile]
           [org.eclipse.jgit.lib
            AbbreviatedObjectId
            AnyObjectId
            Constants
            ObjectDatabase
            ObjectId
            ObjectInserter
            ObjectLoader
            ObjectReader
            ObjectStream
            PersonIdent
            ProgressMonitor
            Ref
            RefDatabase
            RefUpdate
            RefUpdate$Result
            Repository
            RepositoryBuilder
            StoredConfig]
           [org.eclipse.jgit.revwalk RevCommit]
           [org.eclipse.jgit.transport
            PackedObjectInfo
            PackParser
            PackParser$ObjectTypeAndSize]))


(defn not-implemented
  []
  (throw (ex-info "Not yet implemeted"
                  {:reason :not-implemented})))

;;; Object storage

;; These next few methods can become the basis for a protocol that can
;; be extracted once there's more than one place to store objects.

;; TODO: Consider making the interface asynchronous

(defn- ^File obj-file
  "Returns a java.io.File pointing to `obj-name` within file store `store`."
  [file-store obj-name]
  (-> file-store :obj-dir (io/file obj-name)))

(defn cached-obj-bytes
  "Returns the (potentially cached) bytes of object named `obj-name`
  from `store`."
  [store obj-name]
  ;; TODO: Maybe cache
  (Files/toByteArray (obj-file store obj-name)))

(defn ^InputStream obj-stream
  "Returns an InputStream over the object named `obj-name`"
  [store obj-name]
  (java.io.FileInputStream. (obj-file store obj-name)))

(defn write-obj
  "Writes an object into the store under the name `obj-name`."
  [store obj-name ^InputStream data]
  (let [buf-size 10000
        buf      (byte-array buf-size)]
    (with-open [out (java.io.FileOutputStream. (obj-file store obj-name))]
      (loop [bytes-read (.read data buf 0 buf-size)]
        (when (pos? bytes-read)
          (.write out buf 0 bytes-read)
          (recur (.read data buf 0 buf-size)))))))

;; File-based object storage

(defn file-obj-store
  [repo-name]
  ;; TODO: Sanitize repo-name
  (let [obj-dir (io/file "/tmp/daedal/" repo-name)]
    (.mkdirs obj-dir)
    {:obj-dir obj-dir}))

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

(def jgit-type
  {:commit Constants/OBJ_COMMIT
   :tree   Constants/OBJ_TREE
   :blob   Constants/OBJ_BLOB})

(def daedal-type (zipmap (vals jgit-type) (keys jgit-type)))

(defn obj-len
  "Returns the length in bytes of object named `obj-name` in `metastore`."
  [metastore obj-name]
  (ffirst
   (d/q '[:find ?len
          :in $ ?obj-name
          :where
          [?e :object/sha ?obj-name]
          [?e :object/len ?len]]
        (-> metastore :conn d/db)
        obj-name)))

(defn obj-exists?
  "Returns true if an object named `obj-name` exists in `metastore`."
  [metastore obj-name]
  (ffirst
   (d/q '[:find ?e
          :in $ ?obj-name
          :where
          [?e :object/sha ?obj-name]]
        (-> metastore :conn d/db)
        obj-name)))


(defn type-partition
  "Maps a daedal type to the partition where object of that type
  should live"
  [type]
  (or (get {:commit :part/commits
            :blob   :part/blobs
            :tag    :part/tags
            :tree   :part/trees}
           type)
      (throw (ex-info "Unable to determine partition for type"
                      {:reason :no-partition-mapping-defined
                       :type   type}))))

(defn base-name
  [^File f]
  (let [name (.getName f)]
    (.substring name 0 (.lastIndexOf name "."))))

(defn fill
  "Read bytes from `in` and put them in `buf`. Throw if `length` bytes
  cannot be read."
  [^InputStream in ^bytes buf ^long offset ^long length]
  (loop [read-previously 0]
    (let [read-this-time (.read in
                                buf
                                (+ offset read-previously)
                                (- length read-previously))
          read-so-far (+ read-this-time read-previously)]
      (cond
       (= read-so-far length) read-so-far
       (neg? read-this-time) (throw (ex-info "End of stream reached."
                                             {:reason :end-of-stream
                                              :in     in
                                              :buf    buf
                                              :offset offset
                                              :length length}))
       :else (recur read-so-far)))))

(defn read-int32
  "Consumes four bytes from `in` and converts them to a long in
  network byte order."
  [^InputStream in]
  (let [buf (byte-array 4)
        bb (ByteBuffer/allocate 4)
        read (fill in buf 0 4)]
    (log/trace "read-long" :read read)
    (.put bb buf)
    (.position bb 0)
    (.getInt bb)))

(defn consume-pack-header
  [^InputStream in]
  (let [buf (byte-array 4)]
    (when-not (and (= 4 (.read in buf 0 4))
                   (= (into [] buf)
                      (into [] Constants/PACK_SIGNATURE)))
      (throw (ex-info "Pack signature incorrect: should be 'PACK'"
                      {:reason :invalid-pack-signature
                       :buf buf})))
    (log/trace "Pack signature is valid")))

(defn read-pack-version
  [^InputStream in]
  (let [version (read-int32 in)]
    (log/trace "Read pack version" :version version)
    (when-not (= version 2)
      (throw (ex-info (format "Unsupported pack version: %s" version)
                      {:reason :unsupported-pack-version
                       :version version})))
    version))

(defn read-object-count
  [^InputStream in]
  (let [object-count (read-int32 in)]
    (log/trace "Read object count" :object-count object-count)
    object-count))

(def type-from-num
  {Constants/OBJ_COMMIT :commit
   Constants/OBJ_TREE :tree
   Constants/OBJ_BLOB :blob
   Constants/OBJ_TAG :tag
   Constants/OBJ_OFS_DELTA :ofs-delta
   Constants/OBJ_REF_DELTA :ref-delta})

(defn read-type-and-length
  [^InputStream in]
  (let [b0 (.read in)
        type (-> b0 (bit-and 0x70) (bit-shift-right 4) type-from-num)
        initial-length (long (bit-and b0 0x0F))]
    (when (neg? b0)
      (throw (ex-info "Unexpected end of file"
                      {:reason :unexpected-eof})))
    (if-not (bit-test b0 7)
      [type initial-length]
      (do
        (log/trace "Reading type and length" :byte b0)
        (loop [length initial-length
               shift 4
               next-b (.read in)]
          (when (neg? next-b)
            (throw (ex-info "Unexpected end of file"
                            {:reason :unexpected-eof})))
          (log/trace "Reading type and length" :byte next-b)
          (let [new-length (-> next-b (bit-and 0x7f) (bit-shift-left shift) (+ length))]
            (if (bit-test next-b 7)
              (recur new-length (+ shift 7) (.read in))
              [type new-length])))))))

(defn skip-compressed
  "Reads compressed bytes from `in` until done."
  [^InputStream in]
  ;; For now we just skip them. Later we'll do something with them.
  (log/trace "Skipping over compressed data")
  ;; This is not the world's most efficient way to do this, one byte
  ;; at a time. If this turns out to be slow, one approach would be to
  ;; use a BufferedStream, reading chunks of `in` at a time until
  ;; we're done. If we overshoot the end of the compressed region, we
  ;; can use (.getBytesRead inflater) to figure out how much to back
  ;; up, and a .mark/.reset on the BufferedStream to get there.
  (let [inflater (java.util.zip.Inflater.)
        inflated-buf (byte-array 1024)
        deflated-buf (byte-array 1)]
    (loop []
      (when-not (.finished inflater)
        (when (.needsInput inflater)
          (.read in deflated-buf 0 1)
          (.setInput inflater deflated-buf))
        (.inflate inflater inflated-buf 0 1)
        (recur)))))

(defn make-pack-parser
  [metastore ^ObjectDatabase object-database ^InputStream in]
  (proxy [PackParser] [object-database in]
    (onAppendBase [type-code data info] (not-implemented))
    (onBeginOfsDelta [delta-stream-position base-stream-position inflated-size]
      (not-implemented))
    (onBeginRefDelta [delta-stream-position base-id inflated-size]
      (not-implemented))
    (onBeginWholeObject [stream-position type inflated-size] (not-implemented))
    (onEndThinPack [] (not-implemented))
    (onEndWholeObject [^PackedObjectInfo info] (not-implemented))
    (onInflatedObjectData [^PackedObjectInfo info type-code ^bytes data]
      (not-implemented))
    (onObjectData [src raw pos len] (not-implemented))
    (onObjectHeader [src raw pos len] (not-implemented))
    (onPackFooter [hash] (not-implemented))
    (onPackHeader [obj-cnt] (not-implemented))
    (onStoreStream [raw pos len] (not-implemented))
    (readDatabase [dst pos cnt] (not-implemented))
    (seekDatabase [obj-or-delta ^PackParser$ObjectTypeAndSize info] (not-implemented))
    (parse [^ProgressMonitor receiving
            ^ProgressMonitor resolving]
      (log/debug "Entering PackParser.parse")
      (let [_       (consume-pack-header in)
            version (read-pack-version in)
            object-count (read-object-count in)]
        (dotimes [obj-num object-count]
          (let [[type length] (read-type-and-length in)]
            (log/trace "Read object header" :type type :length length)
            (cond
             ;; Just skip over it for now - later we'll do something with it
             (#{:commit :tree :blob} type) (skip-compressed in)

             :else (throw (ex-info "Unsupported object type"
                                   {:reason :unsupported-object-type
                                    :type type
                                    :length length}))))))
      (log/debug "Leaving PackParser.parse"))))

(defn make-jgit-pack-parser
  "This is the JGit-compliant version, which is somewhat insane. Not
  sure I want to use it."
  [metastore object-database in]
  (let [state (atom {})
        crc (java.util.zip.CRC32.)
        temp-file (File/createTempFile "incoming-" ".pack")
        temp-pack-file (java.io.RandomAccessFile. temp-file "rw")]
    (proxy [PackParser] [object-database in]
      (onAppendBase [type-code data info]
        (not-implemented))
      (onBeginOfsDelta [delta-stream-position base-stream-position inflated-size]
        (not-implemented))
      (onBeginRefDelta [delta-stream-position base-id inflated-size]
        (not-implemented))
      (onBeginWholeObject [stream-position type inflated-size]
        (log/debug "onBeginWholeObject"
                   :stream-position stream-position
                   :type (daedal-type type)
                   :inflated-size inflated-size)
        (swap! state #(-> %
                          (assoc-in [:current :offset] stream-position)
                          (assoc-in [:current :type] (daedal-type type))
                          (assoc-in [:current :inflated-size] inflated-size)))
        (.reset crc))
      (onEndThinPack []
        (not-implemented))
      (onEndWholeObject [^PackedObjectInfo info]
        (log/debug "onEndWholeObject" :info info)
        (swap! state #(-> %
                          (assoc-in [:objects (.name info)] (:current %))
                          (dissoc :current)))
        (.setCRC info (unchecked-int (.getValue crc))))
      (onInflatedObjectData [^PackedObjectInfo info type-code ^bytes data]
        (log/debug "onInflatedObjectData"
                   :info info
                   :type (daedal-type type-code)
                   :obj-name (.name info)
                   :data data
                   :len (alength data))
        (swap! state #(-> %
                          (assoc-in [:objects (.name info) :data] data))))
      (onObjectData [src raw pos len]
        (log/debug "onObjectData"
                   :src src
                   :raw raw
                   :pos pos
                   :len len
                   :string-data (String. raw))
        (.update crc raw pos len))
      (onObjectHeader [src raw pos len]
        (log/debug "onObjectHeader"
                   :src src
                   :raw raw
                   :pos pos
                   :len len)
        (swap! state #(-> %
                          (assoc-in [:current :header-length] len)))
        (.update crc raw pos len))
      (onPackFooter [hash]
        (log/debug "onPackFooter"
                   :hash hash)
        (swap! state assoc-in [:pack-hash] hash))
      (onPackHeader [obj-cnt]
        (log/debug "onPackHeader" :obj-cnt obj-cnt))
      (onStoreStream [raw pos len]
        (log/debug "onStoreStream"
                   :raw raw
                   :pos pos
                   :len len)
        (.write temp-pack-file raw pos len))
      (readDatabase [dst pos cnt]
        (log/debug "readDatabase"
                   :dst dst
                   :pos pos
                   :cnt cnt)
        (.read temp-pack-file dst pos cnt))
      (seekDatabase [obj-or-delta ^PackParser$ObjectTypeAndSize info]
        ;; Note that there are two two-arity overloads of this method.
        ;; Need to resolve by type in the body if there are any
        ;; differences.
        (.reset crc)
        (.seek temp-pack-file (.getOffset obj-or-delta))
        (let [info-out (.readObjectHeader ^PackParser this info)]
          (log/debug "seekDatabase"
                     :obj-or-delta obj-or-delta
                     :info-out info-out)
          info-out))
      (parse [^ProgressMonitor receiving
              ^ProgressMonitor resolving]
        (log/debug "PackParser.parse" :stage :before-super)
        (proxy-super parse receiving resolving)
        (log/debug "PackParser.parse"
                   :stage :after-super
                   :temp-pack-file (.getAbsolutePath temp-file)
                   :state @state)

        ;; Write the object data into the object store
        (doseq [[sha {:keys [inflated-size
                             header-length
                             type
                             offset
                             data]}] (:objects @state)]
          (log/trace "Object found"
                     :inflated-size inflated-size
                     :type type
                     :offset offset
                     :data (if data (String. data 0 inflated-size) nil))
          (log/trace "Writing object to object store"
                     :sha sha
                     :type type)
          (write-obj (:obj-store metastore)
                     sha
                     (if data
                       (java.io.ByteArrayInputStream. data)
                       (java.util.zip.InflaterInputStream.
                        (java.nio.channels.Channels/newInputStream
                         (-> temp-pack-file
                             .getChannel
                             (.position (+ offset header-length))))))))

        ;; TODO: Write the object metadata to Datomic
        #_(->> @state
               :objects
               (map (fn [[sha {:keys [inflated-size type offset data]}]]
                      (object-txdata
                       sha
                       type
                       inflated-size
                       data)))
               )

        ;; Now, having parsed the packfile and built an index in
        ;; @state, we can write out any objects that we need to,
        ;; potentially pulling their data from the packfile, if it
        ;; isn't already stored.

        ;; TODO: Get rid of the temp packfile
        ))))

(defn insert-obj
  "Add an object to the repository"
  [metastore ^ObjectInserter inserter type data]
  (not-implemented))

(defn make-object-inserter
  [metastore obj-db]
  (proxy [ObjectInserter] []
    (flush []
      (log/debug "ObjectInserter.flush")
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
      ([type ^bytes data] (insert-obj metastore this type data))
      ([type len in] (insert-obj metastore this type in))
      ([type data off len] (insert-obj metastore this type data off len)))
    (newPackParser [^InputStream in]
      (log/debug "ObjectInserter.newPackParser")
      (make-jgit-pack-parser metastore obj-db in)
      ;;(make-pack-parser metastore obj-db in)
      )
    (release [] ; no-op
      (log/debug "ObjectInserter.release")
      )))

(defn- single
  "Helper function that returns the only thing in a collection. Throws
  if there is not exactly one."
  [coll]
  (or (= 1 (count coll))
      (throw (ex-info "Collection did not have a single item"
                      {:reason :collection-not-singular
                       :coll   coll
                       :count  (count coll)}))))

(def rules
  '[
    ;; ?descendant descends from ?ancestor iff:
    ;; They are the same
    [(descends-from? ?ancestor ?descendant)
     [(= ?ancestor ?descendant)]]
    ;; Or ?ancestor is parent of ?descendant
    [(descends-from? ?ancestor ?descendant)
     [?descendant :commit/parents ?ancestor]]
    ;; Or ?descendant's parents are descendants of ?ancestor
    [(descends-from? ?ancestor ?descendant)
     [?decendant :commit/parents ?parent]
     (descends-from? ?ancestor ?parent)]

    ;; Object with ?sha is in ?repo iff ?repo contains an object
    ;; reachable through one of its refs with that sha.
    [(repo-object ?repo ?sha)
     [?ref :ref/repo ?repo]
     [?ref :ref/target ?target]
     [?obj :object/sha ?sha]
     [(descends-from? ?target ?obj)]]])

(defn obj-type
  "Return the JGit type of an object given its name."
  [metastore obj-name]
  (->> (d/q '[:find ?type
              :in $ % ?repo ?sha
              :where
              [(repo-object ?repo ?sha)]
              [?obj :git/type ?type]]
            (-> metastore :conn d/db)
            rules
            obj-name)
       single
       jgit-type))

(defn make-object-stream
  "Returns an instance of JGit's ObjectStream over the object named by
  `object-id`."
  [metastore obj-name]
  (log/debug "make-object-stream"
             :metastore metastore
             :obj-name obj-name)
  (let [store  (:obj-store metastore)
        _      (or (obj-exists? metastore obj-name)
                   (throw (MissingObjectException.
                           (ObjectId/fromString obj-name)
                           "unknown")))
        stream (obj-stream store obj-name)
        type   (obj-type metastore obj-name)
        len    (obj-len metastore obj-name)]
    (proxy [ObjectStream] []

      ;; ObjectStream-specific methods
      (getSize [] len)
      (getType [] type)

      ;; InputStream passthrough
      (available [] (.available stream))
      (close [] (.close stream))
      (mark [read-limit] (.mark stream read-limit))
      (markSupported [] (.markSupported stream))
      (read
        ([] (.read stream))
        ([b] (.read stream b))
        ([b off len] (.read stream b off len)))
      (reset [] (.reset stream))
      (skip [n] (.skip stream n)))))


(defn make-object-loader
  "Creates an instance of JGit's ObjectLoader for an object with the
  given ID."
  [metastore ^AnyObjectId object-id]
  (log/debug "make-object-loader"
             :metastore metastore
             :object-id object-id)
  (let [obj-name               (.name object-id)
        large-object-threshold 1000000
        obj-store              (:obj-store metastore)]
    (proxy [ObjectLoader] []
      (getType []
        (log/trace "ObjectLoader.getType" :id obj-name)
        (obj-type metastore obj-name))
      (getCachedBytes
        ([] (.getCachedBytes ^ObjectLoader this large-object-threshold))
        ([size-limit]
           (cond
            (not (obj-exists? metastore obj-name))
            (do
              (log/warn "Couldn't find object"
                        :object-id object-id)
              (throw (MissingObjectException. (.copy object-id) "unknown")))

            (< size-limit (obj-len metastore obj-name))
            (throw (org.eclipse.jgit.errors.LargeObjectException. object-id))

            :else
            (cached-obj-bytes obj-store obj-name))))
      (openStream []
        (make-object-stream metastore obj-name)))))

(defn make-object-reader
  [metastore obj-db]
  (proxy [ObjectReader] []
    (getShallowCommits []
      ;; I'm not sure what shallow commits are, but it looks like they
      ;; can be turned off.
      ;;#{}
      (not-implemented)
      )
    (newReader [] (not-implemented))
    (open
      ([^AnyObjectId object-id]
         (log/debug "ObjectReader.open" :object-id object-id)
         (.open ^ObjectReader this object-id ObjectReader/OBJ_ANY))
      ([^AnyObjectId object-id type-hint]
         (log/debug "ObjectReader.open"
                    :object-id object-id
                    :type-hint type-hint)
         ;; TODO: This should actually look in the database, not the
         ;; object store.

         ;; TODO: Throw IncorrectObjectTypeException if the object is
         ;; not of the specified type.
         (when-not (obj-exists? metastore (.name object-id))
           (throw (MissingObjectException. (.copy object-id)
                                           (if (= type-hint ObjectReader/OBJ_ANY)
                                             "unknown"
                                             type-hint))))
         (make-object-loader metastore object-id)))
    (resolve [^AbbreviatedObjectId id] (not-implemented))))

(defn make-object-database
  [metastore]
  (proxy [ObjectDatabase] []
    (close [])                          ; no-op
    (newInserter [] (make-object-inserter metastore this))
    (newReader [] (make-object-reader metastore this))))

(defn only
  "Returns the first element from a collection. Throws if there is
  more than exactly one element."
  [coll]
  (if (next coll)
    (throw (ex-info "Collection contains more than one element."
                    {:reason :aint-only
                     :coll   coll}))
    (first coll)))

(defn make-ref
  "Creates and returns a JGit Ref object if it can be found in the
  metastore. Otherwise, returns nil."
  [metastore ref-name]
  (let [r (only
           (d/q '[:find ?ref
                  :in $ ?repo-name ?ref-name
                  :where
                  [?repo :repo/name ?repo-name]
                  [?ref :ref/repo ?repo]
                  [?ref :ref/name ?ref-name]]
                (-> metastore :conn d/db)
                (:repo-name metastore)
                ref-name))]
    (if-not r
      nil
      (proxy [Ref] []
        (getLeaf [] (not-implemented))
        (getName [] (not-implemented))
        (getObjectId [] ;;(when-let [id (:id r)] (ObjectId/fromString id))
          (not-implemented)
          )
        (getPeeledObjectId [] (not-implemented))
        (getStorage [] (not-implemented))
        (getTarget [] (not-implemented))
        (isPeeled [] (not-implemented))
        (isSymbolic [] (not-implemented))))))

(defn make-new-ref
  "Return an instance of Ref that points to `object-id`. The
  referenced object that may not yet exist, in which case `nil` is
  acceptable for `object-id`."
  [ref-name object-id]
  (proxy [Ref] []
    (getLeaf [] (not-implemented))
    (getName [] ref-name)
    (getObjectId [] object-id)
    (getPeeledObjectId [] (not-implemented))
    (getStorage [] (not-implemented))
    (getTarget [] (not-implemented))
    (isPeeled [] (not-implemented))
    (isSymbolic [] (not-implemented))))

(defn make-ref-update
  [metastore ^Repository repo ^RefDatabase ref-db ref-name]
  (proxy [RefUpdate] [(or (make-ref metastore ref-name)
                          (make-new-ref ref-name nil))]
    (doDelete [desired-result] (not-implemented))
    (doLink [target] (not-implemented))
    (doUpdate [desired-result]
      (let [new-id   (.getNewObjectId this)
            old-id   (.getOldObjectId this)]
        (log/debug "RefUpdate.doUpdate"
                   :new-id new-id
                   :old-id old-id
                   :ref-name ref-name)
        (when old-id
          (throw (ex-info "No support yet for changes to existing refs"
                          {:reason :ref-change-not-supported
                           :new-id new-id
                           :old-id old-id
                           :ref-name ref-name})))
        (let [conn       (:conn metastore)
              object-eid (only
                          (d/q '[:find ?object-eid
                                 :in $ ?object-name
                                 :where
                                 [?object-eid :object/sha ?object-name]]
                               (d/db conn)
                               (.getName new-id)))
              repo-eid   (only
                          (d/q '[:find ?repo
                                 :in $ ?repo-name
                                 :where
                                 [?repo :repo/name ?repo-name]]
                               (d/db conn)
                               (:repo-name metastore)))]
          (log/debug "RefUpdate.doUpdate transacting"
                     :ref/name ref-name
                     :ref/repo repo-eid
                     :ref/target object-eid)
          @(d/transact conn
                       [{:db/id      (d/tempid :part/refs)
                         :ref/name   ref-name
                         :ref/repo   repo-eid
                         :ref/target object-eid}])))
      RefUpdate$Result/NEW)
    (getRefDatabase [] ref-db)
    (getRepository [] repo)
    (tryLock [deref?]
      ;; We handle concurrency in the database and by using a better
      ;; language, so we should be able to allow concurrent access
      ;; regardless.
      true)
    (unlock []
      ;; No-op. Implement if necessary.
      )))

(defn make-ref-map
  "A mutable map of ref names to Ref objects"
  [metastore]
  (let [ref-data (atom {})]
   (proxy [java.util.Map] []
     (clear [] (reset! ref-data {}))
     (containsKey [k] (contains? @ref-data k))
     (containsValue [v] (boolean (some #{v} (vals @ref-data))))
     (entrySet [] (set @ref-data))
     (get [k] (get @ref-data k))
     (isEmpty [] (empty? @ref-data))
     (keySet [] (set (keys @ref-data)))
     ;; TODO: Do we need to write through to the database when this
     ;; gets updated?
     (put [k v] (swap! ref-data assoc k v))
     (putAll [t] (swap! ref-data merge t))
     (remove [k] (swap! ref-data dissoc k))
     (size [] (count @ref-data))
     ;; When ref-data is empty, vals returns nil, but code calling
     ;; this doesn't like that very much. So we return an empty
     ;; collection.
     (values [] (or (vals @ref-data) [])))))

(defn make-ref-database
  [metastore ^Repository repo]
  (proxy [RefDatabase] []
    (close [])                          ; no-op
    (create [] (not-implemented))
    (getAdditionalRefs [] (not-implemented))
    (getRef [name]
      (log/debug "RefDatabase.getRef"
                 :name name))
    (getRefs [prefix] (make-ref-map metastore))
    (isNameConflicting [name]
      ;; TODO: We have to disallow
      ;; refs/heads/master/blah if refs/heads/master already exists,
      ;; and vice-versa.
      (not-implemented))
    (newRename [from-name to-name] (not-implemented))
    (newUpdate [name detach?]
      (log/debug "RefDatabase.newUpdate"
                 :name name
                 :detach? detach?)
      ;; TODO: Deal with symbolic refs properly
      (when detach? (not-implemented))
      (make-ref-update metastore repo this name))
    (peel [ref] (not-implemented))))

(defn mem-repo-builder
  []
  (proxy [RepositoryBuilder] []
    (setup [] (not-implemented))
    (build [] (not-implemented))
    (setGitDir [^File git-dir] (not-implemented))
    (setObjectDirectory [^File object-directory] (not-implemented))
    (addAlternateObjectDirectory [^File other] (not-implemented))
    (setWorkTree [^File work-tree] (not-implemented))
    (setIndexFile [^File index-file] (not-implemented))))

(defn make-stored-config
  [metastore]
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

(defn ^Repository make-repo
  [repo-name datomic]
  (let [metastore {:conn      (-> datomic :uri d/connect)
                   :repo-name repo-name
                   :obj-store (file-obj-store repo-name)}
        obj-db    (make-object-database metastore)]
    (proxy [Repository] [(mem-repo-builder)]
      (create [bare?] (not-implemented))
      (getConfig [] (make-stored-config metastore))
      (getObjectDatabase [] obj-db)
      (getRefDatabase [] (make-ref-database metastore this))
      (getReflogReader [^String ref-name] (not-implemented))
      (notifyIndexChanged [] (not-implemented))
      (scanForRepoChanges [] (not-implemented)))))
