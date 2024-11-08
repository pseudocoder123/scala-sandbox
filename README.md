# Day9 Task - Kafka Consumer

create another microservice
that creates three actors

CloudListener<br/>
NetworkListener<br/>
AppListener<br/>

Each of them should have an associated consumer correspong to the
topic that stores the message

no sooner any of these actor gets a message

they need to pass the message to the

MessageGatherer Actor

This actor has a kakfa producer that stores the received
message to a topic name consilated-messages