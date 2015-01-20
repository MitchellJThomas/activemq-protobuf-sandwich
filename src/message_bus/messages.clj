(ns message-bus.messages
  (:require
   [clojure.core.async :refer [go timeout put! <! chan >!! go-loop]]
   [flatland.protobuf.core :as p]
   [clojure.tools.logging :as log])
  (:import [message_bus PersonProtos$Person]))

(def ^:private proto-person (p/protodef PersonProtos$Person))

(defmulti person-builder
  "Build a Person from either key/values or bytes.  Returns nil upon failure (when bad bytes are provided)"
  class)

(defmethod person-builder (class (byte-array []))
  [^bytes person-bytes]
  (try 
    (p/protobuf-load Person person-bytes)
    (catch com.google.protobuf.InvalidProtocolBufferException e nil)))

(defmethod person-builder (class {})
  [person-map]
  (p/protobuf-dump (p/protobuf proto-person person-map)))

(comment
  (def pm {:id 4 :name "Bob" :email "bob@example.com"}) 

  (def p (p/protobuf proto-person pm))

  (p/protobuf-schema proto-person)

  (def new-p (-> p
               (assoc :likes ["cooking" "swimming" "fishing"] :name "Ralph")
               (p/protobuf-dump)))

  (def ba  (byte-array [(byte 0x43) 
                        (byte 0x6c)
                        (byte 0x6f)
                        (byte 0x6a)
                        (byte 0x75)
                        (byte 0x72)
                        (byte 0x65)
                        (byte 0x21)]))
  (person-builder ba)
  
  (person-builder (person-builder pm))
  )
