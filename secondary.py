import os
import time
import logging
import requests
from flask import Flask, request, jsonify
from model import Message


app = Flask(__name__)
replicated_messages: list[Message] = []
message_ids = set()

logging.basicConfig(level=logging.INFO)

# Load delay and failure configuration
REPLICATION_DELAY = int(os.getenv('DELAY_IN_MS', 0))
FAILURE = os.getenv('FAILURE', False)

logging.info(f"Replication delay: {REPLICATION_DELAY}ms")
logging.info(f"Failure: {FAILURE}")


@app.route('/replicate', methods=['POST'])
def replicate():
    if FAILURE:
        logging.info("Mocked failure, returning 500")
        return jsonify({'status': 'failure'}), 500

    message_body = request.json.get('message')
    message = Message(**message_body)
    time.sleep(REPLICATION_DELAY/1000)

    save_message(message)

    return jsonify({'status': 'success'}), 200


@app.route('/messages', methods=['GET'])
def get_replicated_messages():
    sorted_messages = sorted(
        replicated_messages, key=lambda x: x.sequence_number
    )

    # Detect gaps and include messages up to the first gap
    result = []
    for i in range(len(sorted_messages)):
        this = sorted_messages[i].sequence_number
        prev = sorted_messages[i - 1].sequence_number
        if i > 0 and (this != prev + 1):
            break  # Stop when a gap is detected
        result.append(sorted_messages[i].to_json())

    return jsonify(result), 200


@app.route('/health', methods=['GET'])
def health():
    if FAILURE:
        return jsonify({'status': 'unhealthy'}), 500
    return jsonify({'status': 'healthy'}), 200


def save_message(message: Message):
    if message.sequence_number not in message_ids:
        replicated_messages.append(message)
        message_ids.add(message.sequence_number)
        logging.info(f"Message replicated: {message}")
    else:
        logging.info(f"Duplicate message ignored: {message}")


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5001)
