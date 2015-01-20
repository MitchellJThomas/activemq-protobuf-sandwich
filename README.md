# message-bus

An example of using ActiveMQ and Protobuf with Clojure. 

## Usage

Compile the protobuf file
    $ lein protobuf

Download an
[ActiveMQ broker](http://activemq.apache.org/download.html).  Start it
running e.g. "./activemq start" from the bin directory.

Note: The code found here only uses the openwire port (61616).

Start both a publisher and consumer and watch the log messsage as the
randomly built person objects fly back and forth.

	$ lein trampoline run consumer
	$ lein trampoline run producer

To end consuming/publishing, quit the process with Cntrl-C.
