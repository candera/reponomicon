(ns daedal.common
  "A wretched hive of scum and villany. I.e. the namespace where we
  put things that don't have a better place."
  (:require [clojure.edn :as edn]
            [clojure.tools.logging :as log])
  (:import [java.security MessageDigest]))

(defn digest
  "Returns a string that is an encoding of a cryptographic hash of
  `s`."
  [^String s]
  (let [hash (MessageDigest/getInstance "SHA-256")]
    (.update hash (.getBytes s))
    (let [digest (.digest hash)]
      (apply str (map #(format "%02x" (bit-and % 0xff)) digest)))))

(defn traced-proxy-fn
  [class fn-form]
  (let [[method & tail] fn-form
        overloaded? (-> tail first list?)
        params-and-bodies (if overloaded?
                            tail
                            (list tail))]
    (apply list
           (into [method]
                 (map (fn [[params & body]]
                        `(~params
                          (log/trace "Entering method"
                                     :class ~class
                                     :method ~(name method))
                          (let [result# (do ~@body)]
                            (log/trace "Exiting method"
                                       :class ~class
                                       :method ~(name method)
                                       :result result#)
                            result#)))
                      params-and-bodies)))))

(defmacro traced-proxy
  "Like Clojure's proxy, but wraps every call with a bunch of trace logging."
  [[class & interfaces :as class-and-interfaces] ctor-args & fs]
  `(proxy ~class-and-interfaces ~ctor-args
     ~@(map #(traced-proxy-fn class %) fs)))
