import json
import random
import string
from confluent_kafka import Producer # type: ignore

class KafkaProducerClass:
    def __init__(self, config, logger=None):     
        self.logger = logger or print

        kafka_conf = {
            'bootstrap.servers': config['kafka_bootstrap_servers'],
            'client.id': config.get('client_id', 'python-producer'),

            # 'security.protocol': 'SSL',
            # 'ssl.ca.location': config.get('kafka_ssl_cafile'),
            # 'ssl.certificate.location': config.get('kafka_ssl_certfile'),
            # 'ssl.key.location': config.get('kafka_ssl_keyfile'),

            'enable.idempotence': True,
            'acks': 'all',
            'retries': 10,
            'compression.type': 'gzip',
            'batch.num.messages': 1000,
            'queue.buffering.max.ms': 500,
            'max.in.flight.requests.per.connection': 5,
        }
        self.producer = Producer(kafka_conf)

    def _delivery_report(self, err, msg):
        if err is not None:
            print(f"Delivery failed: {err}")
        else:
            print(f"Delivered message to {msg.topic()} [{msg.partition()}]")

    def send_message(self, topic, key, value):
        self.producer.produce(topic, key=key, value=value, callback=self._delivery_report)

    def flush(self):
        self.producer.flush()

    def send_bulk_random_messages(self, topic, count=10):
        for i in range(count):
            key = f"key-{i}"
            value = self._generate_random_string(20)
            data = json.dumps({ "msg": value})
            self.send_message(topic, key, data)

        self.flush()

    @staticmethod
    def _generate_random_string(length):
        return ''.join(random.choices(string.ascii_letters + string.digits, k=length))
    

class KafkaProducerSingleton:
    _instance = None

    @staticmethod
    def get_instance():
        if KafkaProducerSingleton._instance is None:
            raise Exception("KafkaProducerSingleton is not initialized. Call `initialize` first.")
        return KafkaProducerSingleton._instance

    @staticmethod
    def initialize(config):
        if KafkaProducerSingleton._instance is None:
            KafkaProducerSingleton._instance = KafkaProducerClass(config)