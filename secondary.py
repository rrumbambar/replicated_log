import os
import time
import logging
from flask import Flask, request, jsonify

app = Flask(__name__)
replicated_messages = []
message_ids = set()

logging.basicConfig(level=logging.INFO)

# Load delay and failure configuration
REPLICATION_DELAY = int(os.getenv('DELAY_IN_MS', 0))
FAILURE = os.getenv('FAILURE', False)

logging.info(f"Replication delay: {REPLICATION_DELAY}ms")
logging.info(f"Failure: {FAILURE}")


@app.route('/replicate', methods=['POST'])
def save_message():
    if FAILURE:
        return jsonify({'status': 'failure'}), 500

    message = request.json.get('message')
    time.sleep(REPLICATION_DELAY/1000)
    if message['sequence_number'] not in message_ids:
        replicated_messages.append(message)
        message_ids.add(message['sequence_number'])
        logging.info(f"Message replicated: {message}")
    else:
        logging.info(f"Duplicate message ignored: {message}")

    return jsonify({'status': 'success'}), 200


@app.route('/messages', methods=['GET'])
def get_replicated_messages():
    sorted_messages = sorted(
        replicated_messages, key=lambda x: x['sequence_number'])
    return jsonify(sorted_messages), 200


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5001)
