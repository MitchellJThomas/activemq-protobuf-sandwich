# Apache ActiveMQ and Google Protobuf 
A quickly assembled meal composed of
[Apache ActiveMQ](http://activemq.apache.org) and [Google
Probobuf](https://code.google.com/p/protobuf/).  I call this a
sandwich because its a "quick and dirty" implementation with very few
extra bells and whistles over the existing dependencies.

The most interesting part of this implementation (to me) is the combination of
core.async and ActiveMQ.  It makes switching between message routing
in ActiveMQ (the system) and message routing in Clojure (the process)
more seamless.

Note: Compiling Google Protobuf is not a requirement for using this
code.

## Usage
Compile the example protobuf file

	lein protobuf

Download an
[ActiveMQ broker](http://activemq.apache.org/download.html).  Start it
running e.g. "./activemq start" from the bin directory.

Note: The code found here only uses the openwire port (61616).

Start both a publisher and consumer and watch the log messages as the
randomly built person objects fly back and forth.

	lein trampoline run consumer
	lein trampoline run producer

To end consuming/producing, quit the process with Cntrl-C.
