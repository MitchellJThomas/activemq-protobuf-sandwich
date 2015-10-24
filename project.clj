(defproject activemq-protobuf-sandwich "0.1.0"
  :url "https://github.com/MitchellJThomas/activemq-protobuf-sandwich"
  :description "An example of using Appche ActiveMQ and Google Protobuf with Clojure."
  :dependencies [[org.clojure/clojure "1.7.0"]
                 [org.clojure/core.async "0.1.346.0-17112a-alpha"]
                 [org.apache.activemq/activemq-client "5.12.1"]
                 [org.flatland/protobuf "0.8.1"]
                 [org.clojure/tools.logging "0.3.1"]
                 [org.slf4j/slf4j-log4j12 "1.7.12"]]
  :plugins [[lein-protobuf "0.4.3"]
            [cider/cider-nrepl "0.10.0-SNAPSHOT"]]
  :main ^:skip-aot message-bus.core
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all}
             :dev {:dependencies [[org.apache.activemq/activemq-broker "5.12.1"]]}}
  )
