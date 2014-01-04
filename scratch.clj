(require '[clojure.test :refer (is)])

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
