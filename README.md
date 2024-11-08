# Day9 Task - Kafka Producer

Create a Microservice
that has end point
process-Message

create a case Class Message<br/>
(messageType: string
message: string
messageKey: string (not be confused with partition key))

process-Message should receive Message

There must be Three actors

NetworkMessageProcessor that process the message of type NetworkMessage<br/>
CloudMessageProcessor that process the message of type CloudMessage<br/>
AppMessageProcessor that process the message of type AppMessageProcessor

NetworkMessages are written to topic network-message<br/>
CloudMessages are written to topic cloud-message<br/>
AppMessages are written to topi app-message

May be You think about creating an Actor Named MessageHandler
to pass the message to the right actor