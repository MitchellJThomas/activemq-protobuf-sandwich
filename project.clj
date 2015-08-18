(defproject activemq-protobuf-sandwich "0.1.0"
  :url "https://github.com/MitchellJThomas/activemq-protobuf-sandwich"
  :description "An example of using Appche ActiveMQ and Google Protobuf with Clojure."
  :dependencies [[org.clojure/clojure "1.6.0"]
                 [org.clojure/core.async "0.1.346.0-17112a-alpha"]
                 [org.apache.activemq/activemq-client "5.10.0"]
                 [org.flatland/protobuf "0.8.1"]
                 [org.clojure/tools.logging "0.3.1"]
                 [org.slf4j/slf4j-log4j12 "1.7.10"]]
  :plugins [[lein-protobuf "0.4.1"]]
  :main ^:skip-aot message-bus.core
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all}}
  )
