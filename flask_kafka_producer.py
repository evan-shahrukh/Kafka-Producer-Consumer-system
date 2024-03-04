from flask import Flask, request, jsonify, session
from kafka import KafkaProducer
import json
from datetime import datetime

app = Flask(__name__)
app.secret_key = "your_secret_key"  # Set a secret key for session management

# Initialize Kafka Producer
producer = KafkaProducer(bootstrap_servers='localhost:9092', api_version=(2, 7, 0),
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

@app.route('/')
def home():
    if 'username' in session:
        return jsonify({'message': f'Logged in as {session["username"]}'})
    return jsonify({'message': 'You are not logged in'})

@app.route('/register', methods=['POST'])
def register():
    data = request.get_json()
    if data and 'username' in data and 'password' in data:
        username = data['username']
        # Here, you could add additional validation if required
        session['username'] = username
        # Send user activity message to Kafka
        producer.send('user_activity', {'event_type': 'register', 'username': username, 'timestamp': str(datetime.now())})
        producer.flush()  # Ensure all messages are sent
        return jsonify({'message': 'Registration successful'})
    else:
        return jsonify({'message': 'Username and password are required'}), 400

@app.route('/login', methods=['POST'])
def login():
    data = request.get_json()
    if data and 'username' in data and 'password' in data:
        username = data['username']
        # Here, you could add additional validation if required
        session['username'] = username
        # Send user activity message to Kafka
        producer.send('user_activity', {'event_type': 'login', 'username': username, 'timestamp': str(datetime.now())})
        producer.flush()  # Ensure all messages are sent
        return jsonify({'message': 'Login successful'})
    else:
        return jsonify({'message': 'Username and password are required'}), 400

@app.route('/logout', methods=['POST'])
def logout():
    if 'username' in session:
        username = session.pop('username')
        # Send user activity message to Kafka
        producer.send('user_activity', {'event_type': 'logout', 'username': username, 'timestamp': str(datetime.now())})
        producer.flush()  # Ensure all messages are sent
        return jsonify({'message': 'Logout successful'})
    else:
        return jsonify({'message': 'You are not logged in'}), 401

# Other routes remain unchanged...

if __name__ == '__main__':
    app.run(debug=True)
