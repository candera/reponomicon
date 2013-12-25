(ns user
  (:require [clojure.java.io :as io]
            [clojure.java.javadoc :refer (javadoc)]
            [clojure.repl :refer :all]
            [clojure.tools.namespace.repl :refer (refresh refresh-all)]
            [clojure.tools.trace :refer (trace-ns)]
            [daedal.git :refer :all])
  (:import [java.io ByteArrayInputStream]
           [org.eclipse.jgit.lib
            AbbreviatedObjectId
            AnyObjectId
            CommitBuilder
            Constants
            FileMode
            ObjectDatabase
            ObjectId
            ObjectInserter
            ObjectReader
            PersonIdent
            Ref
            RefDatabase
            RefUpdate
            Repository
            RepositoryBuilder
            TreeFormatter]
           [org.eclipse.jgit.revwalk RevWalk]))

(def cid-a "1234567890123456789012345678901234567890")
(def tid-a "2234567890123456789012345678901234567890")

(defn file-repo
  [^String path]
  (let [git-path (if (.endsWith path ".git")
                   path
                   (str path "/.git"))]
    (org.eclipse.jgit.storage.file.FileRepositoryBuilder/create
     (io/file git-path))))

(defn add-simple-tree
  [^Repository repo ^String ref ^String data]
  (let [parent     (.resolve repo ref)
        ins        (.newObjectInserter repo)
        data-bytes (.getBytes data (java.nio.charset.Charset/forName "UTF-8"))
        blob-id    (.insert ^ObjectInserter ins
                            ^int Constants/OBJ_BLOB
                            ^bytes data-bytes)
        tree       (doto (TreeFormatter.)
                     (.append "file.txt" FileMode/REGULAR_FILE blob-id))
        tree-id    (.insert ins tree)
        craig      (PersonIdent. "Craig Andera" "candera@wangdera.com")
        commit     (doto (CommitBuilder.)
                     (.setAuthor craig)
                     (.setCommitter craig)
                     (.setMessage "Initial commit")
                     (.setTreeId tree-id))
        _          (when parent (.setParentId commit parent))
        commit-id  (.insert ins commit)
        _          (.flush ins)
        ref-update (doto (.updateRef repo ref)
                     (.setNewObjectId commit-id)
                     ;; (.setExpectedOldObjectId (or parent (ObjectId/zeroId)))
                     )]
    (.update ref-update)))
