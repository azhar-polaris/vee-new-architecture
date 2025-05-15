import threading
import logging
from typing import Optional
from confluent_kafka import Producer, KafkaException # type: ignore


logging.basicConfig(level=logging.DEBUG)

class SingletonMeta(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            instance = super().__call__(*args, **kwargs)
            cls._instances[cls] = instance
        return cls._instances[cls]

class KafkaProducer(metaclass=SingletonMeta):
    def __init__(self, cfg, logger=None):
        self.logger = logger or logging.getLogger("KafkaProducer")
        self.partition_count = {}

        conf = {
            "bootstrap.servers": cfg["kafka_bootstrap_servers"],
            "acks": "all",
            "retries": 10,
            "retry.backoff.ms": 500,
            "enable.idempotence": True,
            "batch.size": 100000,
            "linger.ms": 100,
            "queue.buffering.max.messages": 1000000,
            "queue.buffering.max.kbytes": 1048576,
            "message.max.bytes": 1000000,
            "request.timeout.ms": 30000,
            "delivery.timeout.ms": 120000,
            "message.timeout.ms": 120000,
            "compression.type": "gzip",
        }

        if cfg.get("kafka_security_protocol"):
            conf["security.protocol"] = cfg["kafka_security_protocol"]
        if cfg.get("kafka_sasl_username"):
            conf["sasl.username"] = cfg["kafka_sasl_username"]
        if cfg.get("kafka_sasl_password"):
            conf["sasl.password"] = cfg["kafka_sasl_password"]
        if cfg.get("kafka_sasl_mechanism"):
            conf["sasl.mechanism"] = cfg["kafka_sasl_mechanism"]

        self.producer = Producer(conf)
        self._start_polling_thread()

    def _acked(self, err, msg):
        if err is not None:
            self.logger.error(f"Delivery failed: {err}")
        else:
            self.logger.info(f"Delivered message to topic: {msg.topic()} partition: [{msg.partition()}] offset: {msg.offset()}")

    def _start_polling_thread(self):
        def poller():
            while True:
                self.producer.poll(0.5)  # triggers callbacks
        threading.Thread(target=poller, daemon=True).start()

    def produce_message(self, topic: str, message: bytes, key: Optional[str] = None):
        try:
            self.producer.produce(topic=topic, value=message, key=key, callback=self._acked)
            self.logger.debug(f"Produced msg on Kafka Topic: {topic}")
        except BufferError as e:
            self.logger.error(f"Local queue full: {e}")
            raise
        except Exception as e:
            self.logger.error(f"Failed to produce message: {e}")
            raise

    def produce_messages_in_batch(self, topic: str, messages: list[bytes]):        
        for message in messages:
            self.produce_message(topic, message)
        self.logger.debug(f"Produced {len(messages)} messages on topic: {topic}")

    def get_partition_count(self, topic: str, timeout: float = 10.0) -> int:
        if self.partition_count.get(topic) is None:
            try:
                metadata = self.producer.list_topics(topic=topic, timeout=timeout)
                if topic not in metadata.topics:
                    raise KafkaException(f"Topic '{topic}' not found in cluster metadata.")
                
                partition_count = len(metadata.topics[topic].partitions)
                self.partition_count[topic] = partition_count
                return partition_count
            except Exception as e:
                raise RuntimeError(f"Failed to fetch partition count: {e}")
        else:
            return self.partition_count[topic]

    def flush(self, timeout_seconds=10):
        self.producer.flush(timeout_seconds * 1000)

    def close(self):
        self.flush()