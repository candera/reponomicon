(ns daedal.datomic.schema
  "Holds database schema"
  (:require [datomic.api :as d]))

(def ^:private schema-txes
  ;; Schemas. Why quote it? Because tempids change every time you
  ;; evaluate them, and we want a stable representation so we can
  ;; compare it to what's already in the database and only re-assert
  ;; it if anything has changed.
  '[
    ;; General helper functions
    [{:db/ident :user/attr
      :db/doc "Helper fn for creating attributes. type is a bare keyword like :ref,
            :instant, or :string. options is a map of standard Datomic schema
            attributes to be merged in with defaults (cardinality one, indexed
            false)."
      :db/id (d/tempid :db.part/db)
      :db/fn (d/function
              '{:lang :clojure
                :params [db ident type options docstring]
                :code
                [(merge
                  {:db/id (datomic.api/tempid :db.part/db)
                   :db/doc docstring
                   :db/ident ident
                   :db/valueType (keyword "db.type" (name type))
                   :db/cardinality :db.cardinality/one
                   :db.install/_attribute :db.part/db}
                  options)]})}

     {:db/ident :user/part
      :db/doc "Helper fn for creating partitions."
      :db/id (d/tempid :db.part/db)
      :db/fn (d/function
              '{:lang :clojure
                :params [db ident docstring]
                :code
                [{:db/ident ident
                  :db/doc docstring
                  :db/id (datomic.api/tempid :db.part/db)
                  :db.install/_partition :db.part/db}]})}

     {:db/ident :user/ident
      :db/doc "Helper fn for creating generic entities with keyword identities."
      :db/id (d/tempid :db.part/db)
      :db/fn (d/function
              '{:lang :clojure
                :params [db ident docstring]
                :code
                [{:db/ident ident
                  :db/doc docstring
                  :db/id (datomic.api/tempid :db.part/db)}]})}]

    ;; Transacted schema tracking
    [[:user/part :part/schemas
      "Partition for assertions about which schemas are present."]

     [:user/attr :schema/transacted-schema-id :string
      {:db/unique :db.unique/identity}
      "A unique identifier for a particular version of a particular
  schema. Presence of this datom indicates that the corresponding
  schema is present in the database and need not be reasserted."]]

    ;; See also codeq:
    ;; https://github.com/Datomic/codeq/blob/master/src/datomic/codeq/core.clj

    [
     ;; Object partitions
     [:user/part :part/commits
      "Partition for commit entities"]

     [:user/part :part/tags
      "Partition for tag entites"]

     [:user/part :part/trees-and-blobs
      "Partition for tree and blob entities"]

     [:user/part :part/tree-nodes
      "Partition for tree node entities"]

     [:user/part :part/refs
      "Partition for ref entities"]

     [:user/part :part/repos
      "Partition for repo entities"]

     ;; Repository entities

     [:user/attr :repo/name :string
      {:db/unique :db.unique/identity}
      "A git repo uri/name. Something like 'candera/gitomic'."]

     [:user/attr :repo/description :string
      {}
      "A human-readable description of the repository"]

     ;; Ref entities
     [:user/attr :ref/name :string
      {}
      "The name of this reference. E.g. 'refs/heads/master'."]

     [:user/attr :ref/repo :ref
      {}
      "The repository entity to which this ref is scoped."]

     [:user/attr :ref/target :ref
      {}
      "The git object to which this ref refers."]

     ;; Tag entities
     [:user/attr :tag/name :string
      {:db/index true}
      "The name of this tag"]

     [:user/attr :tag/target :ref
      {}
      "The git object to which this tag refers."]

     [:user/attr :tag/tagger-name :string
      {}
      "Name of person who authored the tag."]

     [:user/attr :tag/tagger-email :string
      {}
      "Email of person who authored the tag."]

     [:user/attr :tag/tagged :instant
      {:db/index true}
      "Timestamp of tag."]

     [:user/attr :tag/message :string
      {:db/fulltext true}
      "Tag message."]

     ;; Object entities

     [:user/attr :object/type :keyword
      {}
      "Type enum for git objects - one of :commit, :tree, :blob, :tag"]

     [:user/attr :object/sha :string
      {:db/unique :db.unique/identity}
      "String form of the git sha, in lower-case hexidecimal."]

     [:user/attr :object/len :long
      {}
      "Length of the object in bytes."]

     [:user/attr :commit/parents :ref
      {:db/cardinality :db.cardinality/many}
      "Parents of a commit"]

     [:user/attr :commit/tree :ref
      {}
      "Root node of a commit"]

     [:user/attr :commit/message :string
      {:db/fulltext true}
      "A commit message"]

     [:user/attr :commit/author-name :string
      {}
      "Name of person who authored a commit"]

     [:user/attr :commit/author-email :string
      {}
      "Email of person who author a commit"]

     [:user/attr :commit/authored :instant
      {:db/index true}
      "Timestamp of authorship of commit"]

     [:user/attr :commit/committer-name :string
      {:db/index true}
      "Name of person who committed a commit"]

     [:user/attr :commit/committer-email :string
      {:db/index true}
      "Email of person who committed a commit"]

     [:user/attr :commit/committed :instant
      {:db/index true}
      "Timestamp of commit"]

     [:user/attr :tree-node/path :string
      {:db/index true}
      "Path of a tree node"]

     [:user/attr :tree-node/mode :string
      {}
      "File mode of a tree node"]

     [:user/attr :tree-node/tree :ref
      {}
      "Tree that holds this tree node"]

     ;; I'm not sure we need this one.
     [:user/attr :tree-node/object :ref
      {}
      "Git object (tree/blob) in a tree node"]

     ;; Repo functions

     {:db/ident :ref/update
      :db/doc "Transaction function that atomically moves ref named by `ref-name`
  from pointing at `old-sha` to pointing at `new-sha`. If `old-sha` is
  not the initial value, fails the transaction."
      :db/id (d/tempid :db.part/db)
      :db/fn (d/function
              '{:lang :clojure
                :params [db repo-name ref-name old-sha new-sha]
                :code
                (if-let [ref-eid (ffirst
                                  (datomic.api/q
                                   '[:find ?ref
                                     :in $ ?repo-name ?ref-name ?old-sha
                                     :where
                                     [?repo :repo/name ?repo-name]
                                     [?ref :ref/repo ?repo]
                                     [?ref :ref/name ?ref-name]
                                     [?ref :ref/target ?target]
                                     [?target :object/sha ?old-sha]]
                                   db
                                   repo-name
                                   ref-name
                                   old-sha))]
                  [[:db/add
                    ref-eid
                    :ref/target
                    (d/entid db [:object/sha new-sha])]]
                  (throw (ex-info "Ref did not have expected target"
                                  {:reason    :unexpected-ref-target
                                   :repo-name repo-name
                                   :ref-name  ref-name
                                   :old-sha   old-sha
                                   :new-sha   new-sha})))})}]])

(def schema
  {:txes (eval schema-txes)
   :id (str (hash schema-txes))})
