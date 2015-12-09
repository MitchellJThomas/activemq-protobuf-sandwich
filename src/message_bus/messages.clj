(ns message-bus.messages
  (:require
   [clojure.core.async :refer [go timeout put! <! chan >!! go-loop]]
   [flatland.protobuf.core :as p]
   [clojure.tools.logging :as log])
  (:import [message_bus
            PersonProtos$Person
            PersonProtos$Person$Builder
            PersonProtos$Person$Type]))

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

  ;; go native
  (def bobb (-> (doto (PersonProtos$Person/newBuilder)
                   (.setName "Bob")
                   (.setId 3)
                   (.setEmail "bob@example.com")
                   (.setType PersonProtos$Person$Type/NERVOUS))
                (.build)
                (.toByteArray)))
  (def bob-e (PersonProtos$Person/parseFrom bobb))
  bob-e
  (.getName bob-e)
  (.getDescriptor  (.getType bob-e))

  (clojure.reflect/reflect PersonProtos$Person)
  (clojure.reflect/reflect PersonProtos$Person$Builder)
  (clojure.reflect/reflect PersonProtos$Person$Type)

  )
