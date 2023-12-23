from confluent_kafka import Producer
from faker import Faker
import json
import random
import time

fake = Faker()

producer_config = {
    'bootstrap.servers': 'localhost:9092',  
    'client.id': 'sensor_producer'
}

producer = Producer(producer_config)

def generate_sensor_data(sensor_type, sensor_name):
    return {
        'timestamp': int(time.time()),
        'sensor_type': sensor_type,
        'sensor_name': sensor_name,
        'value': round(random.uniform(0, 100), 2)
    }

def produce_message(sensor_type, sensor_name):
    data = generate_sensor_data(sensor_type, sensor_name)
    print(data)
    producer.produce('new_sensor_data_topic', value=json.dumps(data))
    producer.flush()

def simulate_sensor(sensor_type, sensor_name, delay, count = 2):
    while count > 0:
        produce_message(sensor_type, sensor_name)
        count -= 1
        time.sleep(delay)

if __name__ == '__main__':
    sensor_params = [
    {'type': 'temperature', 'delay': 3},
    {'type': 'pressure', 'delay': 2},
    {'type': 'position', 'delay': 3},
    {'type': 'pressure', 'delay': 4},
    {'type': 'temperature', 'delay': 3}    
    ]

    while True:
       params = random.choice(sensor_params)
       fake_sensor_name = fake.word()
       simulate_sensor(params['type'], fake_sensor_name, params['delay'])
