(ns gitomic.git
  "Implementation of git repo"
  (:refer-clojure :exclude (proxy))
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [gitomic.common
             :as com
             :refer (traced-proxy)
             :rename {traced-proxy proxy}]
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
            PackFile
            PackLock]
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
           [org.eclipse.jgit.revwalk RevCommit RevTag]
           [org.eclipse.jgit.transport
            PackedObjectInfo
            PackParser
            PackParser$ObjectTypeAndSize
            PackParser$UnresolvedDelta]
           [org.eclipse.jgit.util FS]))

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
  (let [obj-dir (io/file "/tmp/gitomic/" repo-name)]
    (.mkdirs obj-dir)
    {:obj-dir obj-dir}))

;; gen-class is another way to do it. This makes interactive
;; development a bit weird, though.

;; (gen-class :name gitomic.git.MemObjectInserter
;;            :state state
;;            :init "init"
;;            :constructors {[Object] []}
;;            :prefix "mem-inserter-")


(def jgit-type
  {:commit Constants/OBJ_COMMIT
   :tree   Constants/OBJ_TREE
   :blob   Constants/OBJ_BLOB
   :tag    Constants/OBJ_TAG})

(def gitomic-type (zipmap (vals jgit-type) (keys jgit-type)))

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

(defn ident-info
  "Given a PersonIdent, return a map with the same info."
  [ident]
  {:name (.getName ident)
   :email (.getEmailAddress ident)
   :when (.getWhen ident)})

(defn parse-commit
  "Return a structured representation of a commit given its raw bytes."
  [data]
  (let [c (RevCommit/parse data)
        committer (.getCommitterIdent c)
        author (.getAuthorIdent c)]
   {:msg (.getFullMessage c)
    :tree (-> c .getTree .getName)
    :parents (map #(.getName %) (.getParents c))
    :author (-> c .getAuthorIdent ident-info)
    :committer (-> c .getCommitterIdent ident-info)}))

(defn parse-tag
  "Return a structured representation of a tag given its raw bytes."
  [data]
  (let [t (RevTag/parse data)
        ti (.getTaggerIdent t)]
    {:target-sha (-> t .getObject .getName)
     :name       (.getTagName t)
     :tagger     (-> t .getTaggerIdent ident-info)
     :msg        (.getFullMessage t)}))

(defn format-sha
  "Turn a 20-byte region of a byte array into a string sha."
  [buf offset]
  (->> (range 20)
       (map #(format "%02x" (aget buf (+ offset %))))
       (apply str)))

(defn parse-tree
  "Return a structed representation of a tree given its raw bytes."
  [data]
  (loop [start 0
         i 0
         entries []]
    (if (>= i (alength data))
      entries
      (if (zero? (aget data i))
        (let [text (String. data start (- i start))
              [mode path] (str/split text #"\s")
              sha (format-sha data (inc i))]
          (recur (+ i 21)
                 (+ i 21)
                 (conj entries {:mode mode :path path :sha sha})))
        (recur start (inc i) entries)))))

(defmulti object-txdata
  "Return transaction data based on object data."
  :type)

(defmethod object-txdata :commit
  [{:keys [sha type repo-eid data tempids db]}]
  (let [{:keys [msg tree parents author committer]}
        (parse-commit data)]
    [{:db/id                  (d/tempid :part/commits (tempids sha))
      :object/type            :commit
      :object/sha             sha
      :object/len             (alength data)
      :object/repo            repo-eid
      :commit/parents         (->> parents
                                   (map (fn [parent-sha]
                                          (if-let [n (tempids parent-sha)]
                                            (d/tempid :part/commits n)
                                            (d/entid db [:object/sha parent-sha]))))
                                   set)
      :commit/author-name     (:name author)
      :commit/author-email    (:email author)
      :commit/authored        (:when author)
      :commit/committer-name  (:name committer)
      :commit/committer-email (:email committer)
      :commit/committed       (:when committer)}]))

(defmethod object-txdata :tree
  [{:keys [sha type repo-eid data tempids db]}]
  (let [tree-sha sha
        tree-eid (d/tempid :part/trees-and-blobs (tempids tree-sha))]
    (->> data
         parse-tree
         (mapcat (fn [{:keys [mode path sha]}]
                   (let [obj-eid (d/tempid :part/trees-and-blobs)]
                     [{:db/id       obj-eid
                       :object/repo repo-eid
                       :object/sha  sha}
                      {:db/id            (d/tempid :part/tree-nodes)
                       :tree-node/mode   mode
                       :tree-node/path   path
                       :tree-node/tree   tree-eid
                       :tree-node/object obj-eid}])))
         (into
          [{:db/id       tree-eid
            :object/type :tree
            :object/sha  tree-sha
            :object/len  (alength data)
            :object/repo repo-eid}]))))

(defmethod object-txdata :blob
  [{:keys [sha type repo-eid inflated-size tempids db]}]
  [{:db/id       (d/tempid :part/trees-and-blobs (tempids sha))
    :object/type :blob
    :object/sha  sha
    :object/len  inflated-size
    :object/repo repo-eid}])

(defmethod object-txdata :tag
  [{:keys [sha type repo-eid data tempids db]}]
  (when (d/entid db [:object/sha sha])
    (throw (ex-info "No support yet for updating existing tags."
                    {:reason :updating-tags-not-supported
                     :sha sha})))
  (let [{:keys [target-sha tagger name msg]} (parse-tag data)]
    [{:db/id            (d/tempid :part/tags (tempids sha))
      :object/type      :tag
      :object/sha       sha
      :object/len       (alength data)
      :object/repo      repo-eid
      :tag/target       (if-let [n (tempids target-sha)]
                          (d/tempid :part/commits n)
                          (d/entid db [:object/sha target-sha]))
      :tag/name         name
      :tag/tagger-name  (:name tagger)
      :tag/tagger-email (:email tagger)
      :tag/tagged       (:when tagger)
      :tag/message      msg}]))

(defn make-jgit-pack-parser
  "This is the JGit-compliant version, which is somewhat insane. Not
  sure I want to use it."
  [metastore object-database in]
  (let [state (atom {})
        crc (java.util.zip.CRC32.)
        temp-file (File/createTempFile "incoming-" ".pack")
        temp-pack-file (java.io.RandomAccessFile. temp-file "rw")]
    (proxy [PackParser] [object-database in]
      (checkCRC [old-crc]
        (-> crc .getValue unchecked-int (= old-crc)))
      (onAppendBase [type-code data info]
        (log/trace "onAppendBase"
                   :type (gitomic-type type-code)
                   :data-type (class data)
                   :info info)
        (swap! state
               update-in
               [:objects (.name info)]
               assoc
               :type (gitomic-type type-code)
               :data data
               :inflated-size (alength data))
        ;; TODO: Returning false here means that this object won't be
        ;; returned from the .getSortedObjectList() method, but given
        ;; that we don't really care about the incoming pack file,
        ;; that seems reasonable, and frees us from having to append
        ;; the object onto the end of the packfile. Appending it isn't
        ;; terrible to do, so if this doesn't work we can go that
        ;; route.
        false)
      (onBeginOfsDelta [delta-stream-position base-stream-position inflated-size]
        (.reset crc))
      (onEndDelta []
        (doto (PackParser$UnresolvedDelta.)
          (.setCRC (unchecked-int (.getValue crc)))))
      (onBeginRefDelta [delta-stream-position base-id inflated-size]
        (.reset crc))
      (onBeginWholeObject [stream-position type inflated-size]
        (log/debug "onBeginWholeObject"
                   :stream-position stream-position
                   :type (gitomic-type type)
                   :inflated-size inflated-size)
        (swap! state #(-> %
                          (assoc-in [:current :offset] stream-position)
                          (assoc-in [:current :type] (gitomic-type type))
                          (assoc-in [:current :inflated-size] inflated-size)))
        (.reset crc))
      (onEndThinPack []
        ;; We don't care about this event because we're not keeping
        ;; the resulting pack file.
        )
      (onEndWholeObject [^PackedObjectInfo info]
        (log/debug "onEndWholeObject" :info info)
        (swap! state #(-> %
                          (assoc-in [:objects (.name info)] (:current %))
                          (dissoc :current)))
        (.setCRC info (unchecked-int (.getValue crc))))
      (onInflatedObjectData [^PackedObjectInfo info type-code ^bytes data]
        (log/debug "onInflatedObjectData"
                   :info info
                   :type (gitomic-type type-code)
                   :obj-name (.name info)
                   :data data
                   :len (alength data))
        (swap! state
               update-in
               [:objects (.name info)]
               assoc
               :data data
               :type (gitomic-type type-code)
               :inflated-size (alength data)))
      (onObjectData [src raw pos len]
        (log/debug "onObjectData"
                   :src src
                   :raw raw
                   :pos pos
                   :len len
                                        ;:string-data (String. raw)
                   )
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
                     :sha sha
                     :inflated-size inflated-size
                     :type type
                     :offset offset
                     :data-type (class data))
          (write-obj (:obj-store metastore)
                     sha
                     (if data
                       (java.io.ByteArrayInputStream. data)
                       (java.util.zip.InflaterInputStream.
                        (java.nio.channels.Channels/newInputStream
                         (-> temp-pack-file
                             .getChannel
                             (.position (+ offset header-length))))))))

        ;; Write the object metadata into Datomic
        (try
          (let [objects (:objects @state)
                tempids (zipmap (map first objects)
                                (range -1 -1000000 -1))
                db (-> metastore :conn d/db)]
            (->> objects
                 (mapcat (fn [[sha {:keys [type data inflated-size]}]]
                           (object-txdata
                            {:sha           sha
                             :type          (or type :blob)
                             :repo-eid      (d/entid
                                             db
                                             [:repo/name (:repo-name metastore)])
                             :data          data
                             :inflated-size inflated-size
                             :tempids       tempids
                             :db            db})))
                 (log/spy :trace)
                 (d/transact (:conn metastore))
                 deref))
          (log/trace "Transacted object metadata")
          (catch Throwable t
            (log/error t "Error transacting object metadata"
                       :state @state)
            (throw t)))

        (.close temp-pack-file)
        ;; TODO: Can we get rid of the temp packfile?
        ;;(.delete temp-file)

        (when-let [lock-message (.getLockMessage this)]
          (log/debug "PackParser.parse" :lock-message lock-message)
          (let [pack-lock (PackLock. temp-file (FS/detect nil))]
            (when-not (.lock pack-lock lock-message)
              (log/error "Could not lock pack file"
                         :lock-message lock-message)
              (throw (ex-info "Could not lock pack file"
                              {:reason       :lock-failure
                               :lock-message lock-message})))
            pack-lock))))))

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

(def rules
  '[
    ;; ?descendant descends from ?ancestor iff:
    ;; They are the same
    [(descends-from? ?ancestor ?descendant)
     [(= ?ancestor ?descendant)]]
    ;; Or it's two commits with a parent-child relationship
    [(descends-from? ?parent ?commit)
     [?commit :commit/parents ?parent]]
    ;; Or it's a tree pointed to by a commit
    [(descends-from? ?commit ?tree)
     [?commit :commit/tree ?tree]]
    ;; Or it's an object pointed to by a tree
    [(descends-from? ?tree ?object)
     [?tree-node :tree-node/tree ?tree]
     [?tree-node :tree-node/object ?object]]
    ;; Or it's connected via a chain of the above
    [(descends-from? ?ancestor ?descendant)
     (descends-from? ?ancestor ?child)
     (descends-from? ?child ?descendant)]

    [(repo-object ?repo ?sha ?obj)
     [?obj :object/sha ?sha]
     [?obj :object/repo ?repo]]])

(defn only
  "Returns the first element from a collection. Throws if there is
  more than exactly one element."
  [coll]
  (if (next coll)
    (throw (ex-info "Collection contains more than one element."
                    {:reason :aint-only
                     :coll   coll}))
    (first coll)))

(defn single
  "Runs the specified query and ensures that it returns a single result."
  [query & inputs]
  (-> (apply d/q query inputs)
      only
      only))

(defn obj-entity
  "Return the Datomic entity map of an object given its name. Returns
  nil if the object is not present."
  [metastore obj-name]
  (let [db (-> metastore :conn d/db)]
    (d/entity db
              (single '[:find ?obj
                        :in $ % ?repo-name ?sha
                        :where
                        [?repo :repo/name ?repo-name]
                        (repo-object ?repo ?sha ?obj)]
                      db
                      rules
                      (:repo-name metastore)
                      obj-name))))

(defn obj-type
  "Return the JGit type of an object given its name."
  [metastore obj-name]
  (->> obj-name (obj-entity metastore) :object/type jgit-type))

(defn obj-len
  "Return the size of an object given its name."
  [metastore obj-name]
  (->> obj-name (obj-entity metastore) :object/len))

(defn make-object-stream
  "Returns an instance of JGit's ObjectStream over the object named by
  `object-id`."
  [metastore obj-name]
  (log/debug "make-object-stream"
             :metastore metastore
             :obj-name obj-name)
  (let [store  (:obj-store metastore)
        _      (or (obj-entity metastore obj-name)
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
      (getSize []
        (log/trace "ObjectLoader.getSize")
        (obj-len metastore obj-name))
      (getType []
        (log/trace "ObjectLoader.getType" :id obj-name)
        (obj-type metastore obj-name))
      (getCachedBytes
        ([] (.getCachedBytes ^ObjectLoader this large-object-threshold))
        ([size-limit]
           (cond
            (not (obj-entity metastore obj-name))
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
      ;; I'm not sure what shallow commits are, but the implementation
      ;;of DfsObjectReader just returns an empty set. We shall do the
      ;;same.
      #{})
    (newReader [] this)
    (open
      ([^AnyObjectId object-id]
         (.open ^ObjectReader this object-id ObjectReader/OBJ_ANY))
      ([object-id type-hint]
         ;; Bah: I can't overload by type - only arity. So I have to
         ;; delegate back to the base class if I can tell that this is
         ;; the other 2-arity overload.
         (if (instance? Iterable object-id)
           (proxy-super open ^Iterable object-id ^int type-hint)
           (do

             ;; TODO: Throw
             ;; IncorrectObjectTypeException if the object is not of the
             ;; specified type.
             (when-not (obj-entity metastore (.name object-id))
               (throw (MissingObjectException. (.copy object-id)
                                               (if (= type-hint ObjectReader/OBJ_ANY)
                                                 "unknown"
                                                 type-hint))))
             (make-object-loader metastore object-id)))))
    (resolve [^AbbreviatedObjectId id] (not-implemented))))

(defn make-object-database
  [metastore]
  (proxy [ObjectDatabase] []
    (close [])                          ; no-op
    (newInserter [] (make-object-inserter metastore this))
    (newReader [] (make-object-reader metastore this))))

(defn make-ref
  "Returns a JGit Ref object given a Datomic ref entity."
  [r]
  (log/trace "make-ref" :r r)
  (proxy [Ref] []
    (getLeaf []
      (log/trace "Ref.getLeaf" :ref-name (:ref/name r))
      ;; For now, don't deal with symbolic refs
      this)
    (getName [] (:ref/name r))
    (getObjectId []
      (->> r
           :ref/target
           ;; Follow symrefs until we run out
           (iterate :ref/target)
           (take-while some?)
           last
           :object/sha
           ObjectId/fromString))
    (getPeeledObjectId []
      (if (= :tag (:object/type r))
        (not-implemented)
        nil))
    (getStorage [] (not-implemented))
    (getTarget [] (not-implemented))
    (isPeeled []
      ;; TODO: revisit this if I ever figure out exactly what a peeled
      ;; tag is.
      false
      )
    (isSymbolic []
      (log/trace "Ref.isSymbolic" :ref-name (:ref/name r))
      (-> r :ref/target :ref/name some?))))

(defn make-refs
  "Creates and returns a sequence of JGit Ref objects. If ref-name is
  supplied limits the results to just that one ref if it can be found
  in the metastore, or nil if cannot."
  [metastore & [ref-name]]
  (let [db       (-> metastore :conn d/db)
        ref-eids (->> (d/q '[:find ?ref
                                :in $ ?repo-name
                                :where
                                [?repo :repo/name ?repo-name]
                                [?ref :ref/repo ?repo]]
                              db
                              (:repo-name metastore))
                      (map first))]
    (log/trace "make-refs" :metastore metastore :ref-name ref-name)
    (->> ref-eids
         (map (fn [e] (d/entity db e)))
         (filter (fn [r] (or (not ref-name)
                             (= ref-name (:ref/name r)))))
         (map make-ref))))

(defn make-new-ref
  "Return an instance of Ref that points to `object-id`. The
  referenced object that may not yet exist, in which case `nil` is
  acceptable for `object-id`."
  [ref-name object-id]
  (log/trace "make-new-ref" :ref-name ref-name :object-id object-id)
  (proxy [Ref] []
    (getLeaf []
      (log/trace "Ref.getLeaf" :ref-name ref-name :object-id object-id)
      ;; For now, don't deal with symbolic refs
      this)
    (getName [] ref-name)
    (getObjectId [] object-id)
    (getPeeledObjectId [] (not-implemented))
    (getStorage [] (not-implemented))
    (getTarget [] (not-implemented))
    (isPeeled [] (not-implemented))
    (isSymbolic []
      (log/trace "Ref.isSymbolic" :ref-name ref-name :object-id object-id)
      ;; For now, don't deal with symbolic refs
      false
      )))

(defn add-new-ref
  [metastore ref-name new-id]
  ;; Add new-ref
  (let [conn       (:conn metastore)]
    (log/debug "RefUpdate.doUpdate transacting"
               :ref-name ref-name
               :metastore metastore
               :new-id new-id)
    @(d/transact conn
                 [[:ref/update
                   (:repo-name metastore)
                   ref-name
                   nil
                   (.getName new-id)]])))

(defn update-ref
  [metastore ref-name old-id new-id]
  @(d/transact (:conn metastore)
               [[:ref/update
                 (:repo-name metastore)
                 ref-name
                 (.getName old-id)
                 (.getName new-id)]]))

(defn delete-ref
  [metastore ref-name]
  (let [conn    (:conn metastore)
        db      (d/db conn)
        ref-eid (ffirst (d/q '[:find ?ref
                               :in $ ?ref-name ?repo-name
                               :where
                               [?repo :repo/name ?repo-name]
                               [?ref :ref/repo ?repo]
                               [?ref :ref/name ?ref-name]]
                             db
                             ref-name
                             (:repo-name metastore)))]
   @(d/transact (:conn metastore)
                [[:db.fn/retractEntity ref-eid]])))

(defn make-ref-update
  [metastore ^Repository repo ^RefDatabase ref-db ref-name]
  (log/trace "make-ref-update" :ref-name ref-name)
  (proxy [RefUpdate] [(or (first (make-refs metastore ref-name))
                          (make-new-ref ref-name nil))]
    (doDelete [desired-result]
      ;; It's not entirely clear to me whether there's any concurrency
      ;; protection needed here. I mean, does it much matter if
      ;; someone else comes along and changes the ref right before you
      ;; get rid of it? One of you has to win.
      (delete-ref metastore ref-name)
      desired-result)
    (doLink [target] (not-implemented))
    (doUpdate [desired-result]
      (try
       (let [new-id   (.getNewObjectId this)
             old-id   (.getOldObjectId this)]
         (log/debug "RefUpdate.doUpdate"
                    :new-id new-id
                    :old-id old-id
                    :ref-name ref-name
                    :desired-result desired-result)
         ;; TODO: I don't really understand how this is supposed to
         ;; work. Something to do with blowing up if the ref is an
         ;; unexpected state, perhaps related to concurrency. But it
         ;; would be nice to handle that at the database level rather
         ;; than this hoky bullshit.
         (.setExpectedOldObjectId this old-id)
         (when-not (or (= desired-result RefUpdate$Result/FORCED)
                       (= desired-result RefUpdate$Result/FAST_FORWARD)
                       (and (= desired-result RefUpdate$Result/NEW)
                            (nil? old-id)))
           (log/error "No support for unforced non-fast forward-changes"
                      :new-id new-id
                      :old-id old-id
                      :ref-name :ref-name
                      :desired-result desired-result)
           (throw (ex-info "No support for unforced non-fast forward changes"
                           {:reason   :non-fast-forward
                            :new-id   new-id
                            :old-id   old-id
                            :ref-name ref-name})))
         (if old-id
           (update-ref metastore ref-name old-id new-id)
           (add-new-ref metastore ref-name new-id)))
       ;; TODO: figure out what the hell this return value ought to be
       desired-result
       (catch Throwable t
         (log/error t "Error in RefUpdate.doUpdate")
         RefUpdate$Result/LOCK_FAILURE)))
    (getRefDatabase []
      (log/trace "RefUpdate.getRefDatabase")
      ref-db)
    (getRepository []
      (log/trace "RefUpdate.getRepository")
      repo)
    (tryLock [deref?]
      (log/trace "Entering RefUpdate.tryLock" :deref? deref?)

      ;; I have no idea why I have to do the following: I copied it
      ;; from what DfsRefUpdate does:

      ;; dstRef = getRef();
      ;; if (deref)
      ;;        dstRef = dstRef.getLeaf();

      ;; if (dstRef.isSymbolic())
      ;;        setOldObjectId(null);
      ;; else
      ;;        setOldObjectId(dstRef.getObjectId());

      ;; return true;

      (let [dest-ref   (.getRef this)
            target-ref (log/spy :trace (if deref? (.getLeaf dest-ref) dest-ref))]
        (.setOldObjectId
         this
         (log/spy :trace
                  (if (.isSymbolic target-ref)
                    nil
                    (.getObjectId target-ref)))))
      (log/trace "Exiting RefUpdate.tryLock")
      true)
    (unlock []
      (log/trace "RefUpdate.unlock (no-op)")
      ;; No-op. Implement if necessary.
      )))

(defn make-ref-map
  "A mutable map of ref names to Ref objects"
  [metastore]
  (let [refs (make-refs metastore)
        ref-map (atom (zipmap (map #(.getName %) refs)
                              refs))]
    (proxy [java.util.Map] []
      (clear [] (not-implemented) (reset! ref-map {}))
      (containsKey [k] (contains? @ref-map k))
      (containsValue [v] (boolean (some #{v} (vals @ref-map))))
      (entrySet [] (set @ref-map))
      (get [k] (get @ref-map k))
      (isEmpty [] (empty? @ref-map))
      (keySet [] (set (keys @ref-map)))
      ;; TODO: Do we need to write through to the database when this
      ;; gets updated?
      (put [k v] (not-implemented) (swap! ref-map assoc k v))
      (putAll [t] (not-implemented) (swap! ref-map merge t))
      (remove [k]
        ;; TODO: Had to implement this one, but still not sure whether
        ;; we have to write through to the database. I think probably
        ;; not. I think the reason this collection is mutable is for
        ;; the convenience of the JGit code, so it can simply remove
        ;; refs like HEAD that it doesn't want to deal with.
        (swap! ref-map dissoc k))
      (size [] (count @ref-map))
      ;; When ref-map is empty, vals returns nil, but code calling
      ;; this doesn't like that very much. So we return an empty
      ;; collection.
      (values [] (or (vals @ref-map) [])))))

(defn make-ref-database
  [metastore ^Repository repo]
  (proxy [RefDatabase] []
    (close [])                          ; no-op
    (create [] (not-implemented))
    (getAdditionalRefs [] (not-implemented))
    (getRef [name]
      (log/debug "RefDatabase.getRef"
                 :name name))
    (getRefs [prefix]
      (make-ref-map metastore))
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
    (peel [ref]
      ;; I know we're supposed to check for whether or not the thing
      ;; is peeled, but that doesn't seem to be totally necessary
      ;; except as a performance enhancement.
      ref)))

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

(defn metastore
  "Construct a metastore instance."
  [repo-name datomic]
  {:conn      (-> datomic :uri d/connect)
   :repo-name repo-name
   :obj-store (file-obj-store repo-name)})

(defn ^Repository make-repo
  [repo-name datomic]
  (let [metastore (metastore repo-name datomic)
        obj-db    (make-object-database metastore)]
    (proxy [Repository] [(mem-repo-builder)]
      (create [bare?] (not-implemented))
      (getConfig [] (make-stored-config metastore))
      (getObjectDatabase [] obj-db)
      (getRefDatabase [] (make-ref-database metastore this))
      (getReflogReader [^String ref-name] (not-implemented))
      (notifyIndexChanged [] (not-implemented))
      (scanForRepoChanges [] (not-implemented)))))
