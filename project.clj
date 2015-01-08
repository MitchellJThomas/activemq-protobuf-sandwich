(defproject message-bus "0.1.0-SNAPSHOT"
  :description "An example of using ActiveMQ and Protobuf with Clojure."
  :url "http://example.com/FIXME"
  :dependencies [[org.clojure/clojure "1.6.0"]]
  :main ^:skip-aot message-bus.core
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all}})
