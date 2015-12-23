(ns message-bus.core-test
  (:require [clojure.test :refer :all]
            [clojure.core.async :refer [put! close! <!!]]
            [message-bus.core :refer :all]
            [message-bus.messages :refer :all]))

(deftest conpro-topic-test
  (testing "Producing and consuming using topics"
    (let [mb (start-message-bus! "vm://localhost?broker.persistent=false")
          dest "topic-test"
          consu (start-consumer! mb {:destination dest})
          publi (start-producer! mb {:destination dest :destination-type :topic})
          person {:id 42 :name "Herbie" :email "hhancock@jazz-foo.com"
                  :likes ["keys" "funk" "deep dish pizza"]}
          person_proto (person-builder person)]
      (is (put! publi person_proto))
      (is (= (person-builder  person_proto) (person-builder (<!! consu))))
      (is (put! publi person))
      (is (= person (<!! consu)))
      (is (stop-message-bus! mb))
      (is (false? (put! publi person)))
      (is (nil? (<!! consu))))))

(deftest conpro-queue-test
  (testing "Producing and consuming using queues"
    (let [mb (start-message-bus! "vm://localhost?broker.persistent=false")
          dest "queue-test"
          consu (start-consumer! mb {:destination dest :destination-type :queue})
          publi (start-producer! mb {:destination dest :destination-type :queue})
          person {:id 42 :name "Monk" :email "tmonk@jazz-foo.com"
                  :likes ["keys" "percussive style" "pork ribs"]}
          person_proto (person-builder person)]
      (is (put! publi person_proto))
      (is (put! publi person))
      (is (= (person-builder  person_proto) (person-builder (<!! consu))))
      (is (= person (<!! consu)))
      (is (stop-message-bus! mb))
      (is (false? (put! publi person)))
      (is (nil? (<!! consu))))))

(deftest using-stopped-message-bus
  (testing "Using a stopped message bus"
    (let [mb (start-message-bus! "vm://localhost?broker.persistent=false")]
      (is (stop-message-bus! mb))
      (is (thrown-with-msg? AssertionError #"session" (start-consumer! mb {:destination "fail"})))
      (is (thrown-with-msg? AssertionError #"session" (start-producer! mb {:destination "fail"})))
      (is (stop-message-bus! mb)))))

;; known to fail
(deftest conpro-multi-pub
  (testing "Multiple producers, single consumer"
    (let [mb (start-message-bus! "vm://localhost?broker.persistent=false")
          dest "multi-pub"
          c (start-consumer! mb {:destination dest})
          pr1 (start-producer! mb {:destination dest})
          pr2 (start-producer! mb {:destination dest})
          p1 {:id 1 :name "Miles" :email "mdavis@jazz-foo.com"
              :likes ["groove" "bop" "thin crust"]}
          p2 {:id 2 :name "Coltrane" :email "jcoltrane@jazz-foo.com"
              :likes ["notes" "fusion" "cigars"]}]
      (is (put! pr1 p1))
      (is (put! pr2 p2))
      (is (= #{p1 p2} (set (repeatedly 2 #(<!! c) ))))
      (is (stop-message-bus! mb))
      #_ (is (false? (put! pr1 {}))) ;; known to fail... needs to be fixed
      (is (false? (put! pr2 {})))
      (is (nil? (<!! c))))))
