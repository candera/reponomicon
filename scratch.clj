() (require '[clojure.test :refer (is)])

(let [repo (mem-repo)
      _ (is (nil? (.resolve repo "HEAD")))
      _ (is (nil? (some-> repo (.getRef "refs/heads/master"))))
      walk (RevWalk. repo )
      _ (is (empty? (into [] walk)))]
)

(let [working-path "/Users/candera/tmp/test2"
      reader (.newObjectReader repo)
      walk (RevWalk. reader)
      head-id (.resolve repo "HEAD")
      head-commit (.parseCommit walk head-id)
      _ (.markStart walk head-commit)
      ]
  (-> walk first .getTree)


  )

[{:path "foo" :children [{:path "bar" :data "bar"}
                         {:path "qux" :children [...]}]}]

[{:path "foo" :data "foo-data"}
 {:path "bar" :data "bar-data"}]

(tree-seq
 (constantly true) :children {:path "" :children [{:path "foo" :children [{:path "bar"}]}]})

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(refresh)
(trace-ns 'daedal.git)
(def mem-db (mem-repo))
(def repo (first mem-db))
(add-simple-tree repo "refs/heads/master" "test data")
(add-simple-tree repo "refs/heads/master" "More stuff")

(proxy [RefUpdate] [nil])

(second mem-db)

(def ins (.newObjectInserter repo))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(refresh)

(def s (jetty-server))
(def start-jetty (:start-fn s))
(def stop-jetty (:stop-fn s))
(start-jetty)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(import '[org.eclipse.jetty.server Server]
        '[org.eclipse.jetty.servlet ServletContextHandler ServletHolder]
        '[org.eclipse.jgit.http.server GitServlet]
        '[org.eclipse.jgit.transport.resolver RepositoryResolver UploadPackFactory]
        '[org.eclipse.jgit.transport UploadPack])

(defn js []
  (let [server         (Server. 3003)
        [repo db]      (mem-repo)
        repo-resolver  (reify org.eclipse.jgit.transport.resolver.RepositoryResolver
                         (open [this req name]
                           (log/debug {:method :repository-resolver/open
                                       :name name})
                           repo))
        upload-factory (proxy [org.eclipse.jgit.transport.resolver.UploadPackFactory] []
                         (create [req repo]
                           (log/debug {:method :upload-pack-factory/create
                                       :request req
                                       :repo repo})
                           (UploadPack. repo)))
        servlet        (doto (GitServlet.)
                         (.setRepositoryResolver repo-resolver)
                         (.setUploadPackFactory upload-factory))
        context        (doto (ServletContextHandler.)
                         (.setContextPath "/")
                         (.addServlet (ServletHolder. servlet) "/*"))]
    (.setHandler server context)
    server))

(def server (js))

(.start server)
(.stop server)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

;; Find all refs

(->> (db)
     (d/q '[:find ?ref
            :where
            [?ref :ref/name]])
     (map first)
     (map #(d/entity (db) %))
     (map d/touch))

;; Find all repos
(->> (db)
     (d/q '[:find ?repo
            :where
            [?repo :repo/name]])
     (map first)
     (map #(d/entity (db) %))
     (map d/touch))

;; Find all objects

(->> (db)
     (d/q '[:find ?obj
            :where
            [?obj :object/sha]])
     (map first)
     (map #(d/entity (db) %))
     (map d/touch))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

;; Working with JGit's stupid packfile API

(let [path "/var/folders/g4/cjxsxcpd4n511t1h5m78m29m0000gn/T/incoming-8633512489875740872.pack"]
  (-> path
      io/file
      (org.eclipse.jgit.internal.storage.file.PackFile. 0)
      .iterator
      (into [])
      (pprint)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

;; Working with stupid packfiles directly

(defn read-object
  [^java.io.InputStream is]
  (let [[type length] (read-type-and-length is)
        _ (println "Type/inflated length" type "/" length)
        inflater (java.util.zip.Inflater.)
        inflated-buf (byte-array (* length 2))
        deflated-buf (byte-array 1)
        deflated-read (atom 0)]
    (println "Read this many:"
             (loop [inflated-read 0]
               (if (.finished inflater)
                 (do
                   (println "Finished?" (.finished inflater))
                   (println "Compressed bytes read" (.getBytesRead inflater))
                   inflated-read)
                 (do
                   (when (.needsInput inflater)
                     (.read is deflated-buf 0 1)
                     (swap! deflated-read inc)
                     (.setInput inflater deflated-buf))
                   (recur (+ inflated-read (.inflate inflater
                                                     inflated-buf
                                                     inflated-read
                                                     1)))))))
    (println "Deflated read" @deflated-read)))


;; Correct output (type/inflated/compressed)
;; :commit 185 122?
;; :tree 38 
;; :blob 3 ??

(let [is (java.io.FileInputStream. "/var/folders/g4/cjxsxcpd4n511t1h5m78m29m0000gn/T/incoming-2978041386142027054.pack")]
  (consume-pack-header is)
  (let [version (read-pack-version is)
        object-count (read-object-count is)]
    (println "Version" version)
    (println "Object count" object-count)
    (dotimes [_ object-count]
      (read-object is))))

(let [raf (java.io.RandomAccessFile. "/var/folders/g4/cjxsxcpd4n511t1h5m78m29m0000gn/T/incoming-2978041386142027054.pack" "rw")
      is (java.nio.channels.Channels/newInputStream (-> raf .getChannel (.position 14)))
      iis (java.util.zip.InflaterInputStream. is)
      buf (byte-array 185)]
  (.read iis buf 0 185)
  (String. buf))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(let [fis (java.io.FileInputStream. "/tmp/daedal/bar/46a4b264acba7c2e835339e505bf3e91ecf40b39")
      buf (byte-array 1000)
      n (.read fis buf)
      data (java.util.Arrays/copyOf buf n)]
  (parse-tree data))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(->> (single '[:find ?ref
               :in $ ?repo-name ?ref-name
               :where
               [?repo :repo/name ?repo-name]
               [?ref :ref/repo ?repo]
               [?ref :ref/name ?ref-name]]
             (db)
             "bar"
             "refs/heads/master")
     (d/entity (db))
     :ref/target
     :object/sha
     ObjectId/fromString)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(def new-sha "33c9b94bee9e336d5e262d68ac88b28e3f222cd6")
(def old-sha "5fde60faaaa0092581cc589b9d5bdabdef34d63d")

@(d/transact (conn) [[:ref/update "bar" "refs/heads/master" old-sha new-sha]])

(datomic.api/q '[:find ?ref
                 :in $ ?repo-name ?ref-name ?old-sha
                 :where
                 [?repo :repo/name ?repo-name]
                 [?ref :ref/repo ?repo]
                 [?ref :ref/name ?ref-name]
                 [?ref :ref/target ?target]
                 [?target :object/sha ?old-sha]]
               (db)
               "bar"
               "refs/heads/master"
               old-sha)

(datomic.api/q '[:find ?obj
                 :in $ %
                 :where
                 [?repo :repo/name ?repo-name]
                 (repo-object ?repo "8936011878241f782acfdf6d9b646b38fa43b675" ?obj)]
               (db)
               rules)

(-> (db) (d/entity [:object/sha "8936011878241f782acfdf6d9b646b38fa43b675"]) d/touch)

(d/q '[:find ?ref-name
       :where
       [?ref :ref/target [:object/sha "38e536b334a94e587fa723b397cd8031b2d1220e"]]
       [?ref :ref/name ?ref-name]]
     (db))

(->> (d/q '[:find ?commit
        :where
        [?commit :commit/parents [:object/sha "8936011878241f782acfdf6d9b646b38fa43b675"]]]
      (db))
    ffirst
    (d/entity (db))
    d/touch)


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(com/traced-proxy-fn 'Foo '(nm [a b] 3))

(macroexpand-1 '(daedal.common/traced-proxy [Foo IBar] [a b]
                                (blerg [a] (+ a 2))
                                (quuxy ([a] (+ a 3))
                                       ([a b] (- a b)))))

(macroexpand-1 '(daedal.common/traced-proxy [org.eclipse.jgit.transport.PackParser]
                                          [object-database in]
    (onAppendBase [type-code data info] (not-implemented))
    (onBeginOfsDelta [delta-stream-position base-stream-position inflated-size]
      (not-implemented))))

(daedal.common/traced-proxy [org.eclipse.jgit.transport.PackParser]
                                          [nil nil]
    (onAppendBase [type-code data info] (not-implemented))
    (onBeginOfsDelta [delta-stream-position base-stream-position inflated-size]
      (not-implemented)))

(clojure.core/proxy
 [org.eclipse.jgit.transport.PackParser]
 [nil nil]
 (onAppendBase
  ([type-code data info]
   (clojure.tools.logging/tracef
    "Entering method"
    :class
    org.eclipse.jgit.transport.PackParser
    :method
    onAppendBase)
   (clojure.core/let
    [result__9107__auto__ (do (not-implemented))]
    (clojure.tools.logging/tracef
     "Exiting method"
     :class
     org.eclipse.jgit.transport.PackParser
     :method
     onAppendBase
     :result
     result__9107__auto__)
    result__9107__auto__)))
 (onBeginOfsDelta
  ([delta-stream-position base-stream-position inflated-size]
   (clojure.tools.logging/tracef
    "Entering method"
    :class
    org.eclipse.jgit.transport.PackParser
    :method
    onBeginOfsDelta)
   (clojure.core/let
    [result__9107__auto__ (do (not-implemented))]
    (clojure.tools.logging/tracef
     "Exiting method"
     :class
     org.eclipse.jgit.transport.PackParser
     :method
     onBeginOfsDelta
     :result
     result__9107__auto__)
    result__9107__auto__))))
