# Introduction to Apache ActiveMQ and Google Protobuf

This bit of code is intended to be an example of how to interact with
an ActiveMQ message bus using Protobuf serialized messages.

These are the features of this project

 1. Leverage core.async APIs to internally (within Clojure space) send
    and process data to and from ActiveMQ topics or queues.
 2. Perform message (de)serialization through channel construction and the features of [transducers](http://clojure.org/transducers)
 3. Provide an example of how to do (de)serialization of Protobuf messages from a core.async channel.
