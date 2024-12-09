from flask import Flask, request, jsonify
import logging
import threading

import schedule
import requests
import asyncio
import aiohttp
import time

from model import Message
from concurrent.futures import ThreadPoolExecutor


from tenacity import retry, stop_after_attempt, wait_exponential

app = Flask(__name__)
messages = []
secondaries = ['http://secondary-a:5001', 'http://secondary-b:5001']

secondary_health = {
    'http://secondary-a:5001': {
        'healthy': True,
        'failed_attempts': 0,
        'status': 'healthy'
    },
    'http://secondary-b:5001': {
        'healthy': True,
        'failed_attempts': 0,
        'status': 'healthy'
    }
}

message_counter = 0
counter_lock = threading.Lock()

logging.basicConfig(level=logging.INFO)

background_executor = ThreadPoolExecutor(max_workers=10)


@app.route('/messages', methods=['POST'])
def send_message():
    message = request.json.get('message')
    write_concern = int(request.json.get('write_concern'))

    if not is_qourum_reached():
        return jsonify({'error': 'No qourum. Master is read-only'}), 503

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
        message_obj = Message(sequence_number=message_counter, message=message)
        messages.append(message_obj)
    logging.info(f"Message added to log: {message_obj}")
    return message_obj


async def replicate_message(message, secondaries, write_concern):
    errors = []
    tasks = []
    for secondary in secondaries:
        task = asyncio.create_task(send_message_to_secondary(secondary, message))
        task.set_name(secondary)  # Set task name here
        tasks.append(task)

    ack_count = 1  # Start with primary's acknowledgment
    pending = set(tasks)

    # Wait for tasks with return_when=FIRST_COMPLETED
    while pending:
        done, pending = await asyncio.wait(
            pending,
            return_when=asyncio.FIRST_COMPLETED
        )

        for task in done:
            secondary, success, error = await task
            if success:
                logging.info(f"Received ACK from {secondary}")
                ack_count += 1
            else:
                logging.error(f"Failed to receive ACK from {secondary}:{error}")
                errors.append(f"Failed to receive ACK from {secondary}:{error}")

        if ack_count >= write_concern:
            # Return success to client, remaining tasks continue in background
            for task in pending:
                task.cancel()
                secondary = task.get_name()  # Task name — secondary URL
                background_executor.submit(
                    asyncio.run,
                    send_message_to_secondary(secondary, message)
                )
                logging.info(f"Sent message to {secondary} in background")
            return True, None

    # If we get here and write_concern wasn't met, return failure
    if ack_count < write_concern:
        return False, errors
    return True, None


def is_write_concern_satisfied(ack_count, write_concern):
    if ack_count >= write_concern:
        logging.info(f"Write concern {write_concern} satisfied")
        return True
    return False


async def send_message_to_secondary(secondary, message):
    try:
        async with aiohttp.ClientSession() as session:
            return await send_message_to_secondary_retry_failed(
                session, secondary, message
            )
    except Exception as e:
        logging.error(f"Failed to send message to {secondary}: {e}")
        return secondary, False, str(e)


@retry(
    stop=stop_after_attempt(20),
    wait=wait_exponential(multiplier=1, min=2, max=16)
)
# Total time: 2 + 2 + 4 + 8 + 16 + 16 + 16 + 16 + 16 + 16 + 16 = 118s
async def send_message_to_secondary_retry_failed(session, secondary, message):
    logging.info(f"Sending message to {secondary}")
    async with session.post(
        f'{secondary}/replicate',
        json={'message': message.to_json()}
    ) as response:
        if response.status == 200:
            return secondary, True, None
        else:
            if not secondary_health[secondary]['healthy']:
                logging.error(f"Secondary {secondary} is unhealthy, not retrying")
                return secondary, False, f"Secondary {secondary} is unhealthy"
            raise Exception(f"HTTP {response.status}: {await response.text()}")


@app.route('/messages', methods=['GET'])
def get_messages():
    as_json = [message.to_json() for message in messages]
    return jsonify(as_json), 200


# If no majority, master will be switched to read-only mode
def is_qourum_reached():
    master = 1
    healthy_nodes = 0 + master
    for secondary in secondaries:
        if secondary_health[secondary]['healthy']:
            healthy_nodes += 1
    return healthy_nodes >= 2


def heartbeat():
    logging.info("Running health check")
    for secondary in secondaries:
        healthy = check_health(secondary)
        secondary_health[secondary]['healthy'] = healthy
        if not healthy:
            secondary_health[secondary]['failed_attempts'] += 1
            if secondary_health[secondary]['failed_attempts'] == 3:
                logging.error(f"Marking {secondary} as unhealthy due to 3 failed attempts")
                secondary_health[secondary]['healthy'] = False
        else:
            secondary_health[secondary]['failed_attempts'] = 0
            secondary_health[secondary]['healthy'] = True


def check_health(secondary):
    try:
        response = requests.get(f'{secondary}/health')
        if response.status_code == 200:
            return True
    except Exception as e:
        logging.error(f"Health check failed for {secondary}: {e}")
    return False


def run_scheduler():
    schedule.every(30).seconds.do(heartbeat)
    while True:
        schedule.run_pending()
        time.sleep(1)


scheduler_thread = threading.Thread(target=run_scheduler, daemon=True)
scheduler_thread.start()

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
