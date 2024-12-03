# Replicated Log

## General info

The Replicated Log system has one Master server and two Secondary servers. Each message received by Master is replicated on every Secondary server.

Master can:
* receive message from client;
* return list of received messages;

Secondary can:
* return list of replicated messages;

## How to run

1. Clone this repository
2. Install the required Python packages: `pip3 install -r requirements.txt`
3. Start Docker containers `docker-compose up --build`
4. If you want to view logs `docker-compose logs`

## How to test

1. send message to master `curl -X POST -H "Content-Type: application/json" -d '{"message": "message_1", "write_concern": 2}' http://localhost:5000/messages`
2. get message from master `curl http://localhost:5000/messages`
3. get message from secondaries `curl http://localhost:5001/messages`, `curl http://localhost:5002/messages`