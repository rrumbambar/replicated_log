# Replicated Log

## General info

The Replicated Log has one Master and any number of Secondaries. Each message received by Master is replicated on every Secondary server.

Master can:
* receive message from client;
* return list of received messages;

Secondary can:
* return list of replicated messages;

## Testing

* send message to master `curl -X POST -H "Content-Type: application/json" -d '{"message": "message_1"}' http://localhost:5000/messages`
* get message from master `curl http://localhost:5000/messages`
* get message from secondary `curl {secondary_url}/messages`