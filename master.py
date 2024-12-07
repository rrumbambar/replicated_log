from flask import Flask, request, jsonify
import logging
import threading

import asyncio
import aiohttp

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
    success, error = asyncio.run(
        replicate_message(message_obj, secondaries, write_concern)
    )
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


async def replicate_message(message, secondaries, write_concern):
    errors = []
    tasks = [
        send_message_to_secondary(secondary, message)
        for secondary in secondaries
    ]

    ack_count = 1  # Start with primary's acknowledgment
    for future in asyncio.as_completed(tasks):
        secondary, success, error = await future
        if success:
            logging.info(f"Received ACK from {secondary}")
            ack_count += 1
        else:
            logging.error(f"Failed to receive ACK from {secondary}: {error}")
            errors.append(f"Failed to receive ACK from {secondary}: {error}")

        # Stop processing if write concern is satisfied
        if is_write_concern_satisfied(ack_count, write_concern):
            break

    # If write concern is not satisfied, return failure
    if ack_count < write_concern:
        return False, errors
    return True, None


def process_individual_response(secondary, response, errors):
    try:
        if response.status_code != 200:
            logging.error(f"Failed to receive ACK from {secondary}")
            errors.append(f"Failed to receive ACK from {secondary}")
            return False
        else:
            logging.info(f"Received ACK from {secondary}")
            return True
    except Exception as e:
        error_message = f"Error communicating with {secondary}: {str(e)}"
        logging.error(error_message)
        errors.append(error_message)
        return False


def is_write_concern_satisfied(ack_count, write_concern):
    if ack_count >= write_concern:
        logging.info(f"Write concern {write_concern} satisfied")
        return True
    return False


async def send_message_to_secondary(secondary, message):
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(f'{secondary}/replicate', json={'message': message}) as response:
                if response.status == 200:
                    return secondary, True, None
                else:
                    return secondary, False, f"HTTP {response.status}"
    except Exception as e:
        return secondary, False, str(e)


@app.route('/messages', methods=['GET'])
def get_messages():
    return jsonify(messages), 200


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
