(ns message-bus.messages
  (:require
   [clojure.core.async :refer [go timeout put! <! chan >!! go-loop]]
   [flatland.protobuf.core :as p]
   [clojure.tools.logging :as log])
  (:import [message_bus PersonProtos$Person]))

(def ^:private proto-person (p/protodef PersonProtos$Person))

(defmulti person-builder
  "Build a Person from either key/values or bytes.  Returns nil upon
  failure (when bad bytes are provided)"
  class)

(defmethod person-builder (class (byte-array []))
  [person-bytes]
  (try 
    (p/protobuf-load proto-person person-bytes)
    (catch com.google.protobuf.InvalidProtocolBufferException e nil)))

(defmethod person-builder clojure.lang.IPersistentMap
  [person-map]
  (p/protobuf-dump (p/protobuf proto-person person-map)))

(comment
  (def pm {:id 3 :name "Bob" :email "bob@example.com"})
  (person-builder (person-builder pm))
  (p/protobuf-schema proto-person)

  (def new-p (-> p
               (assoc :likes ["cooking" "swimming" "fishing"] :name "Ralph")
               (p/protobuf-dump)))
  )
