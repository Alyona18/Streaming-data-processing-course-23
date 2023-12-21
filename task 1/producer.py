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
    producer.produce('sensor_data_topic', key=sensor_name, value=json.dumps(data))
    producer.flush()

def simulate_sensor(sensor_type, sensor_name, delay, distribution):
    while True:
        produce_message(sensor_type, sensor_name)
        time.sleep(delay)

if __name__ == '__main__':
    sensor_params = [
        {'type': 'temperature', 'name': 'temperature_sensor_1', 'delay': 5, 'distribution': 'normal'},
        {'type': 'pressure', 'name': 'pressure_sensor_1', 'delay': 10, 'distribution': 'uniform'}
    ]

    for params in sensor_params:
        simulate_sensor(params['type'], params['name'], params['delay'], params['distribution'])
