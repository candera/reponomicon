(ns gitomic.ssh
  "Implementation of the git ssh endpoints."
  (:require [clojure.tools.logging :as log]
            [com.stuartsierra.component :as component]
            [datomic.api :as d]
            [gitomic.datomic :as datomic])
  (:import [java.io
            ByteArrayInputStream
            ByteArrayOutputStream
            ObjectInputStream
            ObjectOutputStream]
           [org.apache.sshd SshServer]
           [org.apache.sshd.server Command CommandFactory PasswordAuthenticator]
           [org.apache.sshd.server.command UnknownCommand]
           [org.apache.sshd.common Factory KeyPairProvider]
           [org.apache.sshd.common.channel ChannelOutputStream]
           [org.apache.sshd.common.util SecurityUtils]
           [org.apache.sshd.server.keyprovider SimpleGeneratorHostKeyProvider]
           [org.apache.sshd.server.shell ProcessShellFactory]))

(defn command-handler
  "TODO"
  [command]
  (throw (ex-info "not yet implemented"
                  {:reason :not-yet-implemented})))

(defn create-ssh-host-keys
  "Returns a byte array that is the serialized form of a freshly generated KeyPair."
  []
  (let [baos (ByteArrayOutputStream.)
        oos (ObjectOutputStream. baos)]
    (->> "DSA"
         SecurityUtils/getKeyPairGenerator
         .generateKeyPair
         (.writeObject oos))
    (.toByteArray baos)))

(defn create-or-get-key
  "Returns the ssh host key from the database, creating and storing it
  if it's not already present."
  [conn]
  (-> (datomic/create conn
                      :gitomic.ssh/host-keys
                      :gitomic.ssh.host-keys/bits
                      create-ssh-host-keys)
      ByteArrayInputStream.
      ObjectInputStream.
      .readObject))

(defn datomic-keypair-provider
  "Returns an object implementing
  `org.apache.sshd.common.KeyPairProvider` that stores keys in
  Datomic, generating them if necessary."
  [conn]
  (reify KeyPairProvider
    (loadKeys [this]
      [(create-or-get-key conn)])
    (loadKey [this type]
      (when (= type KeyPairProvider/SSH_DSS)
        (create-or-get-key conn)))
    (getKeyTypes [this]
      KeyPairProvider/SSH_DSS)))

(defrecord GitomicSshServer [^SshServer server port datomic]
  component/Lifecycle
  (start [this]
    (let [server (SshServer/setUpDefaultServer)
          conn   (-> datomic :uri d/connect)]
      (.setPort server port)
      (.setKeyPairProvider server (datomic-keypair-provider (-> datomic :uri d/connect)))
      (.setCommandFactory server (reify CommandFactory
                                   (createCommand [this command]
                                     (command-handler command))))
      ;; TODO: Switch to publickey authenticator
      (.setPasswordAuthenticator server
                                 (reify PasswordAuthenticator
                                   (authenticate [this username password session] true)))
      (.start server)
      (assoc this :server server)))
  (stop [this]
    (.stop server)
    this))

(defn server
  "Returns an object implementing the Lifecycle protocl for an ssh
  server. `options` is a map that can contain the following keys:

  :port - The port to run the ssh server on.
  :datomic - An instance of a Datomic Lifecycle component."
  [port]
  (map->GitomicSshServer {:port port}))

