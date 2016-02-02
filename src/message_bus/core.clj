(ns message-bus.core
  (:require
   [clojure.edn]
   [clojure.core.async :refer [go timeout put! <! >! chan >!! <!! close! take! go-loop]]
   [clojure.tools.logging :as log])
  (:import
   [javax.jms MessageListener Session BytesMessage TextMessage]
   [org.apache.activemq ActiveMQConnectionFactory]
   [org.apache.activemq.command ActiveMQTopic ActiveMQQueue])
  (:gen-class))

;; TODO
;; 1. create a separate producer session (allow thread separation to service producer and consumer functions)
;; 2. Allow client code choice of auto-ack or not
;; 3. Re-use existing prod-cons channels

(def ^:private default-url "nio://0.0.0.0:61616")

(defrecord MessageBus [connection session channels])

(defn make-message-bus
  ([] (->MessageBus nil nil {}))
  ([connection session] (map->MessageBus {:connection connection :session session :channels {}})))

(defn start-message-bus!
  [broker & {username :username password :password max-connections :max-connections :or {max-connections 1}}]
  {:pre [(.contains broker "://")]}
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
  {:pre [(= (type @message-bus) MessageBus)]}
  (let [{:keys [session connection channels]} @message-bus
        {:keys [producers consumers]} channels]
    ;; signal the pub-loops to stop
    (run! #(close! (:pub-chan %)) (vals producers))
    ;; signal the consumers work is stopped
    (run! close! (vals consumers))
    ;; wait until all the publisher loops are done doing work
    ;; have have stopped before yanking the rug out from underneath them
    (run! #(<!! (:pub-loop %)) (vals producers))
    ;; yank the rug out
    (if session (.close session))
    (if connection (.close connection))
    (reset! message-bus (make-message-bus))))

(defn start-consumer!
  "Returns a chan subscribed to the destination using the provided
  session. The chan will close when the session is terminated.
  Destination type supports keywords :topic and :queue"
  [message-bus {:keys [destination destination-type] :or {destination-type :topic}}]
  {:pre [(= (type @message-bus) MessageBus)
         (:session @message-bus)
         destination
         (contains? #{:topic :queue} destination-type)]}
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
  the message-bus has closed. Destination type supports keywords :topic and :queue."
  [message-bus {:keys [destination destination-type] :or {destination-type :topic}}]
  {:pre [(= (type @message-bus) MessageBus)
         destination
         (:session @message-bus)
         (contains? #{:topic :queue} destination-type)]}
  (let [cha (chan)
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
    (swap! message-bus #(assoc-in % [:channels :producers #{destination destination-type}] {:pub-chan cha :pub-loop pub-loop} ))
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
        dest "person"
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
            pub (start-producer! sess {:destination dest})]
        (log/info "Created a producer process on topic 'person'")
        (go-loop []
          (let [p (nth people (rand-int (count people)))
                data (str (assoc p :id (rand-int 100)))]
            (>! pub data)
            (<! (timeout 1000))
            (recur)))
        (hook-shutdown! #(do (deliver lock :release)
                             (log/info "Shutting down the producer")
                             (stop-message-bus! sess)))
        (deref lock)
        (System/exit 0))

      (= f "consumer")
      (let [sess (start-message-bus! default-url)
            con (start-consumer! sess {:destination dest})]
        (log/info "Created a consumer process on topic 'person'")
        (go-loop []
          (when-let [person-edn (<! con)]
            (log/info "person: " (clojure.edn/read-string person-edn))
            (recur)))
        (hook-shutdown! #(do (deliver lock :release)
                             (log/info "Shutting down the consumer")
                             (stop-message-bus! sess)))
        (deref lock)
        (System/exit 0))

      :else
      (log/info "Good day. Please provide either 'consumer' or 'producer' as an argument"))))


(comment
  (def ses (start-message-bus! "vm://localhost?broker.persistent=false" :username "fredly" :password "foo"))
  ses
  (def consumer (go-loop []
                  (when-let [m (<! c)]
                    (log/info "this: " m)
                    (recur))))

  (>!! p "hey!")
  (>!! p [{:this (rand) :that (rand)} [1 2 3] {:ding true :dong false}])

  (require '[message-bus.messages :as m])
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
  (stop-message-bus! ses)
  )
