(ns gitomic.ssh
  "Implementation of the git ssh endpoints."
  (:require [clojure.string :as str]
            [clojure.tools.logging :as log]
            [com.stuartsierra.component :as component]
            [datomic.api :as d]
            [gitomic.datomic :as datomic]
            [gitomic.git :as git])
  (:import [java.io
            ByteArrayInputStream
            ByteArrayOutputStream
            ObjectInputStream
            ObjectOutputStream]
           [org.apache.sshd SshServer]
           [org.apache.sshd.server
            Command
            CommandFactory
            PasswordAuthenticator
            PublickeyAuthenticator]
           [org.apache.sshd.server.command UnknownCommand]
           [org.apache.sshd.common Factory KeyPairProvider]
           [org.apache.sshd.common.channel ChannelOutputStream]
           [org.apache.sshd.common.util SecurityUtils]
           [org.apache.sshd.server.keyprovider SimpleGeneratorHostKeyProvider]
           [org.apache.sshd.server.shell ProcessShellFactory]))

(defn normalize-repo-name
  "Remove the leading slash and otherwise clean up anything about the
  incoming repo name that needs to be cleaned up."
  [r]
  (if (.startsWith r "/")
    (subs r 1)
    r))

(defn git-receive-pack
  "Runs the receive-pack command"
  [in out err conn repo-name]
  (git/receive-pack (git/make-repo (normalize-repo-name repo-name)
                                   conn)
                    in out err))

(defn git-upload-pack
  "Runs the upload-pack command"
  [in out err conn repo-name]
  (git/upload-pack (git/make-repo (normalize-repo-name repo-name)
                                  conn)
                   in out err))

(defn command
  "Returns an instance of Command that calls f in a future with the
  input, output, and error streams."
  [f conn params]
  (let [state (atom {})]
   (reify Command
     (destroy [this] (log/trace "destroy"))
     (setErrorStream [this err]
       (log/trace "setErrorStream")
       (swap! state assoc :err err))
     (setExitCallback [this cb]
       (log/trace "setExitCallback")
       (swap! state assoc :exit-cb cb))
     (setInputStream [this in]
       (log/trace "setInputStream")
       (swap! state assoc :in in))
     (setOutputStream [this out]
       (log/trace "setOutputStream")
       (swap! state assoc :out out))
     (start [this env]
       (future
         (try
           (apply f (:in @state) (:out @state) (:err @state) conn params)
           (.onExit (:exit-cb @state) 0)
           (catch Throwable t
             (log/error t "Error invoking ssh command")
             (.onExit (:exit-cb @state) 1 (.getMessage t)))))))))

(defn command-handler
  "Bridges from incoming ssh commands to git operations"
  [conn args]
  (log/trace "command-handler" :args args)
  (let [[command-name & params] (as-> args ?
                                      (str/split ? #"\s")
                                      (remove str/blank? ?))
        normalized-params       (map #(str/replace % #"^'(.*)'$" "$1") params)]
    (log/trace "ssh command received"
               :command command
               :params params
               :normalized-params normalized-params)
    (case command-name
      "git-receive-pack" (command git-receive-pack conn normalized-params)
      "git-upload-pack" (command git-upload-pack conn normalized-params)
      (UnknownCommand. args))))

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
          conn   (-> datomic :uri d/connect)
          user-auth-factories (.getUserAuthFactories server)]
      (.setPort server port)
      (.setKeyPairProvider server (datomic-keypair-provider conn))
      (.setCommandFactory server (reify CommandFactory
                                   (createCommand [this command]
                                     (log/trace "createCommand" :command command)
                                     (command-handler conn command))))
      ;; TODO: Figure out at what point to only allow certain public keyspe
      (.setPublickeyAuthenticator server
                                  (reify PublickeyAuthenticator
                                    (authenticate [this username key session] true)))
      (.start server)
      (assoc this :server server)))
  (stop [this]
    (.stop server)
    this))

(defn server
  "Returns an object implementing the Lifecycle protocl for an ssh
  server. `options` is a map that can contain the following keys:

  :port - The port to run the ssh server on."
  [port]
  (map->GitomicSshServer {:port port}))
