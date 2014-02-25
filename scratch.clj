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
