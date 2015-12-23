(ns message-bus.core
  (:require
   [clojure.edn]
   [clojure.core.async :refer [go timeout put! <! >! chan >!! close! take! go-loop]]
   [clojure.tools.logging :as log]
   [message-bus.messages :as m])
  (:import
   [javax.jms MessageListener Session BytesMessage TextMessage]
   [org.apache.activemq ActiveMQConnectionFactory]
   [org.apache.activemq.command ActiveMQTopic ActiveMQQueue])
  (:gen-class))

;; TODO
;; 1. Allow for queues and topics
;; 2. create a separate publisher session (allow thread separation to service publisher and consumer functions)
;; 3. Allow client code choice of auto-ack or not
;; 4. Re-use existing pub-sub channels

(def ^:private default-url "nio://0.0.0.0:61616")

(defrecord MessageBus [connection session channels])

(defn make-message-bus
  ([] (->MessageBus nil nil {}))
  ([connection session] (map->MessageBus {:connection connection :session session :channels {}})))

(defn start-message-bus!
  [broker & {username :username password :password max-connections :max-connections :or {max-connections 1}}]
  "Returns an ActiveMQ connection which has been started.
     It currently supports the following optional named
     arguments (refer to ActiveMQ docs for more details about them): :username, :password"
  (when (nil? broker) (throw (IllegalArgumentException. "No value specified for broker URL!")))
  (let [f (ActiveMQConnectionFactory. broker)
        c (.createConnection f username password)
        _ (.start c)
        s (.createSession c false Session/AUTO_ACKNOWLEDGE)]
    (atom (make-message-bus c s))))

(defn stop-message-bus!
  [message-bus]
  (let [{:keys [session connection channels]} @message-bus
        {:keys [publishers consumers]} channels]
    (dorun (map close! (vals publishers)))
    (dorun (map close! (vals consumers)))
    (.close session)
    (.close connection)
    (reset! message-bus (make-message-bus))))

(defn start-consumer!
  "Returns a chan subscribed to the destination using the provided
  session. The chan will close when the session is terminated.
  Destination type supports keywords :topic and :destination"
  [message-bus {:keys [destination destination-type] :or {destination-type :topic}}]
  (let [ch (chan)
        ;; Kinda crazy here... the key to the channel type is a set
        _ (swap! message-bus #(assoc-in % [:channels :consumers #{destination destination-type}] ch))
        s (:session @message-bus)
        ml (reify MessageListener
             (onMessage [this mess]
               (cond
                 (instance? BytesMessage mess)
                 (let [ba (byte-array (.getBodyLength mess))
                       _ (.readBytes mess ba)]
                   (>!! ch ba))

                 (instance? TextMessage mess)
                 (let [tx (.getText mess)
                       dat (clojure.edn/read-string tx)]
                   (>!! ch dat))

                 :else (log/warn "Unhandled message" mess))
               (.acknowledge mess)
               (if (.isClosed s) (close! ch))))
        dest (if (= destination-type :topic)
               (ActiveMQTopic. destination)
               (ActiveMQQueue. destination))
        consumer (.createConsumer s dest ml)]
    ch))

(defmulti activemq-message
  "Build an ActiveMQ message based on the outgoing data type"
  (fn [data ses] (class data)))

(defmethod activemq-message :default
  [data sess]
  (.createTextMessage sess (prn-str data)))

(defmethod activemq-message (class (byte-array []))
  [data sess]
  (doto (.createBytesMessage sess)
             (.writeBytes data)))

(defn start-producer!
  "Returns a chan for which puts will be published on the
  provided destination using the given message-bus.  The chan will close when
  the message-bus has closed. Destination type supports keywords :topic and :destination."
  [message-bus {:keys [destination destination-type] :or {destination-type :topic}}]
  (let [cha (chan)
        _ (swap! message-bus #(assoc-in % [:channels :publishers #{destination destination-type}] cha))
        ses (:session @message-bus)
        dest (if (= destination-type :topic)
               (ActiveMQTopic. destination)
               (ActiveMQQueue. destination))
        pub (.createProducer ses dest)
        pub-loop (go
                   (loop []
                     (when-let [mes (<! cha)]
                       (.send pub (activemq-message mes ses))
                       (if (.isClosed ses) (close! cha))
                       (recur)))
                   true)]
    cha))

(defn- hook-shutdown!
  [f]
  (doto (Runtime/getRuntime)
    (.addShutdownHook (Thread. f))))

(defn -main
  "Subscribe and publish messages using a message bus"
  [& args]
  (let [f (first args)
        lock (promise)
        people [{:name "Mitch" :email "mwhite@foo.com"
                 :likes ["laying around" "beer" "liver and onions"]}
                {:name "Julie" :email "jpoodle@foo.com"
                 :likes ["tv" "wine" "dogs"]}
                {:name "Lorrie" :email "lorrie@bar.com"
                 :likes ["cars" "rain"]}
                {:name "Mac" :email "mac@bar.com"
                 :likes ["grass" "gardening" "greens"]}
                {:name "Spock" :email "spock@enterprise.com"
                 :likes ["the nerve pinch" "logic" "Capt. Kirk"]}
                {:name "Capt. Kirk" :email "captain@enterprise.com"
                 :likes ["the ladies" "adventure" "tribbles"]}]]
    (cond
      (= f "producer")
      (let [sess (start-message-bus! default-url)
            pub (start-producer! sess "person")]
        (log/info "Created a publisher process on topic 'person'")
        (go-loop []
          (let [p (nth people (rand-int (count people)))
                wid (assoc p :id (rand-int 100))
                data (m/person-builder wid)]
            (>! pub data)
            (<! (timeout 1000))
            (recur)))
        (hook-shutdown! #(deliver lock :release))
        (hook-shutdown! #(log/info "Shutting down the publisher"))
        (hook-shutdown! #(stop-message-bus! sess))
        (deref lock)
        (System/exit 0))

      (= f "consumer")
      (let [sess (start-message-bus! default-url)
            con (start-consumer! sess "person")]
        (log/info "Created a consumer process on topic 'person'")
        (go-loop []
          (when-let [person-bytes (<! con)]
            (log/info "person: " (m/person-builder person-bytes))
            (recur)))
        (hook-shutdown! #(deliver lock :release))
        (hook-shutdown! #(log/info "Shutting down the consumer"))
        (hook-shutdown! #(stop-message-bus! sess))
        (deref lock)
        (System/exit 0))

      :else
      (log/info "Good day. Please provide either 'consumer' or 'publisher' as an argument"))))


(comment
  (def ses (start-message-bus! "vm://localhost?broker.persistent=false" :username "fredly" :password "foo"))
  ses
  (def consumer (go-loop []
                  (when-let [m (<! c)]
                    (log/info "this: " m)
                    (recur))))

  (>!! p "hey!")
  (>!! p [{:this (rand) :that (rand)} [1 2 3] {:ding true :dong false}])

  (def person-consumer (go-loop []
                         (when-let [person-bytes (<! pcon)]
                           (log/info "m: " person-bytes)
                           (log/info "person: " (m/person-builder person-bytes))
                           (recur))))

  (def p-bytes (m/person-builder {:id 1 :name "Mitch" :email "mthomas@thisnthat.com"
                                  :likes ["biking" "skiing" "futsal" "music"]}))
  (>!! ppub p-bytes)
  (close! pcon)
  (close! ppub)

  (close! c)
  (stop-activemq-session! ses)

  
  (def cc (start-consumer! ses   {:destination "/foo"}))
  cc
  ses
  (def cq (start-consumer! ses   {:destination "/foo" :destination-type :queue}))
  (def pc (start-publisher! ses  {:destination "/foo"}))
  (def pq (start-publisher! ses  {:destination "/foo" :destination-type :queue}))


  )
