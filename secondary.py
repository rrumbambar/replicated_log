import time
import logging
from flask import Flask, request, jsonify

app = Flask(__name__)
replicated_messages = []

logging.basicConfig(level=logging.INFO)

@app.route('/replicate', methods=['POST'])
def save_message():
    message = request.json.get('message')
    time.sleep(5)
    replicated_messages.append(message)
    logging.info(f"Message replicated: {message}")
    return jsonify({'status': 'success'}), 200

@app.route('/messages', methods=['GET'])
def get_replicated_messages():
    return jsonify(replicated_messages), 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5001)
