(ns daedal.common
  "A wretched hive of scum and villany. I.e. the namespace where we
  put things that don't have a better place."
  (:require [clojure.edn :as edn])
  (:import [java.security MessageDigest]))

(defn digest
  "Returns a string that is an encoding of a cryptographic hash of
  `s`."
  [s]
  (let [hash (MessageDigest/getInstance "SHA-256")]
    (.update hash (.getBytes s))
    (let [digest (.digest hash)]
      (apply str (map #(format "%02x" (bit-and % 0xff)) digest)))))

