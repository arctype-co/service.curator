(ns arctype.service.curator
  (:require
    [clojure.tools.logging :as log]
    [arctype.service.protocol :refer :all]
    [arctype.service.util :refer [rmerge]]
    [schema.core :as S]
    [sundbry.resource :as resource]))

(def Config
  {:zk-address S/Str})

(def ^:private default-config
  {})

(defrecord CuratorService [config]
  PLifecycle

  (start [this]
    (log/info {:message "Starting Curator service"})
    this)

  (stop [this]
    (log/info {:message "Stopping Curator service"}))
  
  )

(S/defn create
  [resource-name
   config :- Config]
  (let [config (rmerge default-config config)]
    (resource/make-resource
      (map->CuratorService
        {:config config})
      resource-name)))
