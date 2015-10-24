(ns message-bus.core-test
  (:require [clojure.test :refer :all]
            [clojure.core.async :refer [put! close! <!!]]
            [message-bus.core :refer :all]
            [message-bus.messages :refer :all]))

(deftest pubsub-test
  (testing "Publishing and Consuming using a local broker"
    (let [sess (start-activemq-session! "vm://localhost?broker.persistent=false")
          dest "pubsub-test"
          consu (start-consumer! sess dest)
          publi (start-publisher! sess dest)
          person {:id 42 :name "Herbie" :email "hhancock@jazz-foo.com"
                  :likes ["keys" "funk" "deep dish pizza"]}
          person_proto (person-builder person)]
      (is (put! publi person_proto))
      (is (= (person-builder  person_proto) (person-builder (<!! consu))))
      (is (put! publi person))
      (is (= person (<!! consu)))
      (is (stop-activemq-session! sess))
      (is (false? (put! publi person)))
      (is (nil? (<!! consu))))))
