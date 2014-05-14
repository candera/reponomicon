(ns gitomic.git.object-storage.file
  "Stores git objects in files"
  (:require [clojure.java.io :as io]
            [gitomic.git.object-storage :as storage])
  (:import [com.google.common.io Files]
           [java.io
            File
            FileInputStream
            FileOutputStream
            InputStream]))

(defn- ^File obj-file
  "Returns a java.io.File pointing to `obj-name` within file store `store`."
  [file-store obj-name]
  (-> file-store :obj-dir (io/file obj-name)))

(defrecord FileObjectStore [obj-dir]
  storage/ObjectStorage
  (obj-stream [store obj-name]
    (FileInputStream. (obj-file store obj-name)))

  (write-obj [store obj-name data]
    (let [buf-size 10000
          buf      (byte-array buf-size)]
      (with-open [out (FileOutputStream. (obj-file store obj-name))]
        (loop [bytes-read (.read data buf 0 buf-size)]
          (when (pos? bytes-read)
            (.write out buf 0 bytes-read)
            (recur (.read data buf 0 buf-size))))))))

(defn create-store
  [base-path]
  (let [obj-dir (io/file base-path "objects")]
    (.mkdirs obj-dir)
    (map->FileObjectStore {:obj-dir obj-dir})))

