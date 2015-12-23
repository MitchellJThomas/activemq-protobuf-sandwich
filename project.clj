(defproject mitchelljthomas/activemq-protobuf-sandwich "0.3.0"
  :url "https://github.com/MitchellJThomas/activemq-protobuf-sandwich"
   :license {:name "The MIT License"
            :url "http://opensource.org/licenses/MIT"}
  :description "A thin Clojar wrapper for Apache ActiveMQ. A study using ActiveMQ and Google Protobuf together."
  :dependencies [[org.clojure/clojure "1.7.0"]
                 [org.clojure/core.async "0.2.374"]
                 [org.apache.activemq/activemq-client "5.13.0"]
                 [org.flatland/protobuf "0.8.1"]
                 [org.clojure/tools.logging "0.3.1"]
                 [org.slf4j/slf4j-log4j12 "1.7.13"]]
  :plugins [[lein-protobuf "0.4.1"]]
  :main ^:skip-aot message-bus.core
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all}
             :dev {:dependencies [[org.apache.activemq/activemq-broker "5.13.0"]]}})

