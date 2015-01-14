(ns message-bus.core
  (:require
   [clojure.core.async :refer [go timeout put! <! chan >! close!]]
   [clojure.tools.logging :as log])
  (:import
   [javax.jms MessageListener Session BytesMessage TextMessage]
   [org.apache.activemq ActiveMQConnectionFactory]
   [org.apache.activemq.command ActiveMQTopic])
  (:gen-class))

(defn -main
  "I don't do a whole lot ... yet."
  [& args]
  (log/info "Hello, World!"))

(comment
  (defn start-activemq-session!
    [broker & {username :username password :password max-connections :max-connections :or {max-connections 1}}]
    "Returns an ActiveMQ connection which has been started.
     It currently supports the following optional named
     arguments (refer to ActiveMQ docs for more details about them):
     :username, :password"
    (when (nil? broker) (throw (IllegalArgumentException. "No value specified for broker URL!")))
    (let [connection (doto (ActiveMQConnectionFactory. broker) (.createConnection username password))
          _ (.start connection)
          session (.createSession connection false Session/AUTO_ACKNOWLEDGE)]
      {:connection  connection
       :session session}))

  (defn stop-activemq-session!
    [session]
    (.close :session session)
    (.close (:connection session))
    true)

  (defn create-activemq-chan
    [session topic-name]
    (let [ch (chan)
          s (:session session)
          ml (reify MessageListener
               (onMessage [this mess]
                 (let [cl (class mess)]
                   (cond
                     (instance? BytesMessage cl) (let [ba (byte-array (.getBodyLength mess))
                                                                 _ (.readBytes mess ba)]
                                                             (>! ch ba))
                     (instance? TextMessage cl) (let [tx (.getText mess)]
                                                            (>! ch tx)))
                   (.acknowledge mess)
                   (if (.isClosed s) (close! ch)))))
          topic (ActiveMQTopic. topic-name)
          consumer (.createConsumer s topic ml)]
      ch))
  )
