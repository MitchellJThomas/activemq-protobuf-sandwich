(ns message-bus.core
  (:require
   [clojure.edn]
   [clojure.core.async :refer [go timeout put! <! >! chan >!! close! take! go-loop]]
   [clojure.tools.logging :as log]
   [message-bus.messages :as m])
  (:import
   [javax.jms MessageListener Session BytesMessage TextMessage]
   [org.apache.activemq ActiveMQConnectionFactory]
   [org.apache.activemq.command ActiveMQTopic])
  (:gen-class))

(def ^:private default-url "nio://0.0.0.0:61616")

(def ^:private people
  [{:name "Mitch" :email "mwhite@foo.com"
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
    :likes ["the ladies" "adventure" "tribbles"]}
   ])

(defn start-activemq-session!
  [broker & {username :username password :password max-connections :max-connections :or {max-connections 1}}]
  "Returns an ActiveMQ connection which has been started.
     It currently supports the following optional named
     arguments (refer to ActiveMQ docs for more details about them):
     :username, :password"
  (when (nil? broker) (throw (IllegalArgumentException. "No value specified for broker URL!")))
  (let [factory (ActiveMQConnectionFactory. broker)
        connection (.createConnection factory username password)
        _ (.start connection)
        session (.createSession connection false Session/AUTO_ACKNOWLEDGE)]
    (atom {:connection  connection
           :session session
           :channels []})))

(defn stop-activemq-session!
  [sess]
  (let [{:keys [session connection channels]} @sess]
    (dorun (map close! channels))
    (.close session)
    (.close connection)
    true))
(defn start-consumer!
  "Returns a chan subscribed to the provided topic using the provided
  session.  The chan will close when the session has been terminated."
  [session topic-name]
  (let [ch (chan)
        _ (swap! session #(assoc % :channels (conj (% :channels) ch)))
        s (:session @session)
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

                 :else (log/warn "Unhandled message " mess))
               (.acknowledge mess)
               (if (.isClosed s) (close! ch))))
        topic (ActiveMQTopic. topic-name)
        consumer (.createConsumer s topic ml)]
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

(defn start-publisher!
  "Returns a publishing chan for which puts will be published on the
  provided topic using the given session.  The chan will close when
  the session has closed."
  [session topic-name]
  (let [cha (chan)
        _ (swap! session #(assoc % :channels (conj (% :channels) cha)))
        ses (:session @session)
        top (ActiveMQTopic. topic-name)
        pub (.createPublisher ses top)
        _ (go
            (loop []
              (when-let [mes (<! cha)]
                (.publish pub (activemq-message mes ses))
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
        lock (promise)]
    (cond
      (= f "publisher")
      (let [sess (start-activemq-session! default-url)
            pub (start-publisher! sess "person")]
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
        (hook-shutdown! #(stop-activemq-session! sess))
        (deref lock)
        (System/exit 0))

      (= f "consumer")
      (let [sess (start-activemq-session! default-url)
            con (start-consumer! sess "person")]
        (log/info "Created a consumer process on topic 'person'")
        (go-loop []
          (when-let [person-bytes (<! con)]
            (log/info "person: " (m/person-builder person-bytes))
            (recur)))
        (hook-shutdown! #(deliver lock :release))
        (hook-shutdown! #(log/info "Shutting down the consumer"))
        (hook-shutdown! #(stop-activemq-session! sess))
        (deref lock)
        (System/exit 0))

      :else
      (log/info "Good day. Please provide either 'consumer' or 'publisher' as an argument"))))


(comment
  (def ses (start-activemq-session! default-url :username "fredly" :password "foo"))
  (start-activemq-session! default-url)
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

  (def pu (.createPublisher (:session ses) (ActiveMQTopic. "pine")))
  (.publish pu (.createTextMessage  (:session ses) (str {:this (rand) :that (rand)})))
  (.getConnectionInfo  (:connection  ses))
  (.getTopic pu)
  )
