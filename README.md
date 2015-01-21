# Apache ActiveMQ and Google Protobuf 
A quickly assembled meal composed of
[Apache ActiveMQ](http://activemq.apache.org/download.html) and [Google
Probobuf](https://code.google.com/p/protobuf/).  I call this a
sandwich because its a "quick and dirty" implementation with very few
extra bells and whistles over the existing dependencies.

## Usage
Compile the example protobuf file

	lein protobuf

Download an
[ActiveMQ broker](http://activemq.apache.org/download.html).  Start it
running e.g. "./activemq start" from the bin directory.

Note: The code found here only uses the openwire port (61616).

Start both a publisher and consumer and watch the log messsage as the
randomly built person objects fly back and forth.

	lein trampoline run consumer
	lein trampoline run producer

To end consuming/publishing, quit the process with Cntrl-C.
