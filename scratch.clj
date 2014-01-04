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
