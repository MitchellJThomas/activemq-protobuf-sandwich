(ns message-bus.core
  (:require
   [clojure.core.async :refer [go timeout put! <! chan >!! close! take! go-loop]]
   [clojure.tools.logging :as log])
  (:import
   [javax.jms MessageListener Session BytesMessage TextMessage]
   [org.apache.activemq ActiveMQConnectionFactory]
   [org.apache.activemq.command ActiveMQTopic])
  (:gen-class))

(def ^:private default-url "nio://0.0.0.0:61616")

(defn -main
  "Subscribe and publish messages using a message bus"
  [& args]
  (log/info "Publishing text on topic " (if-let [f (first args)] f default-url) ))

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
    {:connection  connection
     :session session}))

(defn stop-activemq-session!
  [{:keys [session connection]}]
  (do (.close session)
       (.close connection)
       true))

(defn message-bus-subscriber
  "Returns a chan subscribed to the provided topic using the provided
  session.  The chan will close when the session has been terminated."
  [session topic-name]
  (let [ch (chan)
        s (:session session)
        ml (reify MessageListener
             (onMessage [this mess]
               (cond
                 (instance? BytesMessage mess) (let [ba (byte-array (.getBodyLength mess))
                                                     _ (.readBytes mess ba)]
                                                 (>!! ch ba))
                 (instance? TextMessage mess) (let [tx (.getText mess)]
                                                (>!! ch (read-string tx)))
                 :else (log/warn "Unhandled message " mess))
               (.acknowledge mess)
               (if (.isClosed s) (close! ch))))
        topic (ActiveMQTopic. topic-name)
        consumer (.createConsumer s topic ml)]
    ch))

(defn message-bus-publisher
  "Returns a publishing chan for which puts will be published on the
  provided topic using the given session.  The chan will close when the session has closed."
  [session topic-name]
  (let [cha (chan)
        ses (:session session)
        top (ActiveMQTopic. topic-name)
        pub (.createPublisher ses top)
        _ (go
            (loop []
              (when-let [mes (<! cha)]
                (let [txt-mes (.createTextMessage ses mes)]
                  (.publish pub txt-mes)
                  (if (.isClosed ses) (close! cha))
                  (recur))))
            true)
        ]
    cha))

(comment
  (def ses (start-activemq-session! default-url :username "mthomas" :password "foo"))
  (def c (message-bus-subscriber ses "pine"))
  (def p (message-bus-publisher ses "pine"))

  (def pu (.createPublisher (:session ses) (ActiveMQTopic. "pine")))
  (.getConnectionInfo  (:connection  ses))
  (.getTopic p)

  (go-loop []
    (when-let [m (<! c)]
      (log/info "this: " (:this m))
      (recur)))
  
  (.publish p (.createTextMessage  (:session ses) (str {:this (rand) :that (rand)})))

  (>!! p (str {:this (rand) :that (rand)}))
  
  (close! c)
  (stop-activemq-session! ses)
  )
