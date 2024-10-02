from flask import Flask, request, jsonify
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
    success, error = replicate_message(message,secondaries)
    if not success:
        return jsonify({'error': f'Replication failed: {error}'}), 500
    return jsonify({'status': 'success'}), 200

def replicate_message(message, secondaries):
    for secondary in secondaries:
        try:
            response = requests.post(f'{secondary}/replicate', json={'message': message})
            if response.status_code != 200:
                logging.error(f"Failed to receive ACK from {secondary}")
                return False, f"Failed to receive ACK from {secondary}"
            logging.info(f"Received ACK from {secondary}")
        except requests.exceptions.RequestException as e:
            logging.error(f"Error communicating with {secondary}: {str(e)}")
            return False, f"Error communicating with {secondary}: {str(e)}"
    return True, None    


@app.route('/messages', methods=['GET'])
def get_messages():
    return jsonify(messages), 200


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
