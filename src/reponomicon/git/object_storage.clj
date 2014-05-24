(ns reponomicon.git.object-storage
  "Abstraction over various ways to store object bytes.")

;; TODO: Consider making the interface asynchronous

(defprotocol ObjectStorage
  (obj-stream ^java.io.InputStream [store obj-name]
    "Return an InputSTream over the object named `obj-name`.")
  (write-obj [store obj-name ^java.io.InputStream data]
    "Writes an object into the store under the name `obj-name`."))

