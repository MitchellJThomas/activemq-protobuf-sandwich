# Introduction to message-bus

This bit of code is intended to be an example of how to interact with
an ActiveMQ message bus using Protobuf serialized messages.

These are pieces to this code base
1. Subscribing to ActiveMQ topics via core.async channels
2. Publising to ActiveMQ topics using a function
3. Deserializing Protobuf messages from a core.async channel.

It is intended to use the ActiveMQ portion of this library can be used
without the Protobuf portion.
