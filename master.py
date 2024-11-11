from flask import Flask, request, jsonify
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
import logging

app = Flask(__name__)
messages = []
secondaries = ['http://secondary-a:5001', 'http://secondary-b:5001']

logging.basicConfig(level=logging.INFO)


@app.route('/messages', methods=['POST'])
def send_message():
    message = request.json.get('message')
    messages.append(message)
    logging.info(f"Message received: {message}")
    success, error = replicate_message(message, secondaries)
    if not success:
        return jsonify({'error': f'Replication failed: {error}'}), 500
    return jsonify({'status': 'success'}), 200


def replicate_message(message, secondaries):
    errors = []
    futures = send_messages_concurrently(message, secondaries)
    process_responses(futures, errors)
    return (False, errors) if errors else (True, None)


def send_messages_concurrently(message, secondaries):
    with ThreadPoolExecutor() as executor:
        futures = {executor.submit(
            send_message_to_secondary, secondary, message): secondary for secondary in secondaries}
    return futures


def process_responses(futures, errors):
    for future in as_completed(futures):
        secondary = futures[future]
        try:
            response = future.result()
            if response.status_code != 200:
                logging.error(f"Failed to receive ACK from {secondary}")
                errors.append(f"Failed to receive ACK from {secondary}")
            else:
                logging.info(f"Received ACK from {secondary}")
        except Exception as e:
            error_message = f"Error communicating with {secondary}: {str(e)}"
            logging.error(error_message)
            errors.append(error_message)


def send_message_to_secondary(secondary, message):
    return requests.post(f'{secondary}/replicate', json={'message': message})


@app.route('/messages', methods=['GET'])
def get_messages():
    return jsonify(messages), 200


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
