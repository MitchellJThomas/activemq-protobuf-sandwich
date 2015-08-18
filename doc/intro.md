# Introduction to Apache ActiveMQ and Google Protobuf

This bit of code is intended to be an example of how to interact with
an ActiveMQ message bus using Protobuf serialized messages.

These are the features of this code base
1. Subscribing to and consuming from ActiveMQ topics/queues via core.async channels
2. Message (de)serialization through channel construction and the
features of [transducers](http://clojure.org/transducers)
3. An example of how to do (de)serialization of Protobuf messages from
a core.async channel.
