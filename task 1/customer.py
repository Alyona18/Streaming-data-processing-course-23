from confluent_kafka import Consumer, KafkaError
import json
import pandas as pd
import time

consumer_config = {
    'bootstrap.servers': 'localhost:9092',  
    'group.id': 'sensor_aggregator',
    'auto.offset.reset': 'earliest'
}


consumer = Consumer(consumer_config)
consumer.subscribe(['new_sensor_data_topic'])

sensor_data = []

def process_data():
    global sensor_data

    if not sensor_data:
        return

    df = pd.DataFrame(sensor_data, columns=['timestamp', 'sensor_type', 'sensor_name', 'value'])
    
    avg_by_type = df.groupby('sensor_type')['value'].mean()

    avg_by_name = df.groupby('sensor_name')['value'].mean()

    print("\nAverage by Sensor Type:")
    print(avg_by_type)

    print("\nAverage by Sensor Name:")
    print(avg_by_name)

    sensor_data = []

start_time = time.time()
while True:
    msg = consumer.poll(5.0)

    if msg is None:
        continue
    if msg.error():
        if msg.error().code() == KafkaError._PARTITION_EOF:
            continue
        else:
            print(msg.error())
            break

    data = json.loads(msg.value().decode('utf-8'))
    sensor_data.append(data)

    if time.time() - start_time >= 20:
        process_data()
        start_time = time.time()

    time.sleep(1)  
