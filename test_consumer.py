from kafka import KafkaConsumer
from dotenv import load_dotenv
import os
import sys
load_dotenv()

broker_url = os.getenv('BROKER_URL', 'localhost:29092')
topic = os.getenv('TOPIC', '')

consumer = KafkaConsumer(topic, bootstrap_servers=broker_url)

def kafkastream():
    for message in consumer:
        print(message)
        print('frame size %s' % sys.getsizeof(message))

if __name__ == '__main__':
    kafkastream()