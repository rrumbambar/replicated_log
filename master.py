from flask import Flask, request, jsonify
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
import logging
import threading

app = Flask(__name__)
messages = []
secondaries = ['http://secondary-a:5001', 'http://secondary-b:5001']
message_counter = 0
counter_lock = threading.Lock()

logging.basicConfig(level=logging.INFO)


@app.route('/messages', methods=['POST'])
def send_message():
    message = request.json.get('message')
    write_concern = int(request.json.get('write_concern'))
    message_obj = add_message_to_log(message)
    logging.info(f"Message received: {message_obj}")
    success, error = replicate_message(message_obj, secondaries, write_concern)
    if not success:
        return jsonify({'error': f'Replication failed: {error}'}), 500
    return jsonify({'status': 'success'}), 200


def add_message_to_log(message):
    global message_counter
    with counter_lock:
        message_counter += 1
        message_obj = {"sequence_number": message_counter, "message": message}
        messages.append(message_obj)
    logging.info(f"Message added to log: {message_obj}")
    return message_obj


def replicate_message(message, secondaries, write_concern):
    errors = []
    futures = send_messages_concurrently(message, secondaries)
    process_responses(futures, errors, write_concern)
    return (False, errors) if errors else (True, None)


def send_messages_concurrently(message, secondaries):
    with ThreadPoolExecutor() as executor:
        futures = {executor.submit(
            send_message_to_secondary, secondary, message): secondary for secondary in secondaries}
    return futures


def process_responses(futures, errors, write_concern):
    ack_count = 1
    for future in as_completed(futures):
        secondary = futures[future]
        ack_count = process_individual_response(
            future, secondary, errors, ack_count)
        if is_write_concern_satisfied(ack_count, write_concern):
            break


def process_individual_response(future, secondary, errors, ack_count):
    try:
        response = future.result()
        if response.status_code != 200:
            logging.error(f"Failed to receive ACK from {secondary}")
            errors.append(f"Failed to receive ACK from {secondary}")
        else:
            logging.info(f"Received ACK from {secondary}")
            ack_count += 1
    except Exception as e:
        error_message = f"Error communicating with {secondary}: {str(e)}"
        logging.error(error_message)
        errors.append(error_message)
    return ack_count


def is_write_concern_satisfied(ack_count, write_concern):
    if ack_count >= write_concern:
        logging.info(f"Write concern {write_concern} satisfied")
        return True
    return False


def send_message_to_secondary(secondary, message):
    return requests.post(f'{secondary}/replicate', json={'message': message})


@app.route('/messages', methods=['GET'])
def get_messages():
    return jsonify(messages), 200


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
