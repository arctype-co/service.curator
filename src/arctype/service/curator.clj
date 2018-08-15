(ns arctype.service.curator
  (:require
    [clojure.tools.logging :as log]
    [arctype.service.protocol :refer :all]
    [arctype.service.util :refer [rmerge]]
    [curator.framework :refer [curator-framework]]
    [curator.mutex :as curator-mutex]
    [schema.core :as S]
    [sundbry.resource :as resource]))

(def Config
  {:zk-address S/Str
   (S/optional-key :prefix) S/Str})

(def ^:private default-config
  {:prefix "/curator"})

(defn path
  [{{prefix :prefix} :config} subpath]
  (str prefix subpath))

(def curator-path path)

(defmacro curator-locking
  [lock timeout-ms & body]
  `(let [lock# ~lock]
     (log/debug {:message "Acquiring curator lock"})
     (let [aq# (curator-mutex/acquire lock# ~timeout-ms)]
       (log/debug {:message "Acquired curator lock"
                   :result (pr-str aq#)}))
     (try 
       (do ~@body)
       (finally
         (log/debug {:message "Releasing curator lock"})
         (curator-mutex/release lock#)))))

(defrecord CuratorService [config curator]
  PLifecycle

  (start [this]
    (log/info {:message "Starting Curator service"})
    (assoc this :curator (doto 
                           (curator-framework (:zk-address config))
                           (.start))))

  (stop [this]
    (log/info {:message "Stopping Curator service"})
    (.close curator)
    (dissoc this :curator))

  PClientDecorator
  (client [this] curator)
  
  )

(S/defn create
  [resource-name
   config :- Config]
  (let [config (rmerge default-config config)]
    (resource/make-resource
      (map->CuratorService
        {:config config})
      resource-name)))
