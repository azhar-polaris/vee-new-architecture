from typing import Optional
import logging
from confluent_kafka import KafkaException # type: ignore

class KafkaConsumerFactory:
    def __init__(self, base_config: dict, logger: Optional[logging.Logger] = None):
        self.base_config = base_config
        self.logger = logger or logging.getLogger("KafkaConsumerFactory")

    def create_consumer(self, group_id: str):
        config = self.base_config.to_kafka_config()
        config["group.id"] = group_id
        config.setdefault("auto.offset.reset", "earliest")
        config.setdefault("session.timeout.ms", 60000)
        config.setdefault("heartbeat.interval.ms", 3000)
        config.setdefault("max.poll.interval.ms", 300000)

        try:
            from .consumer import KafkaConsumer  # Or wherever your class is defined
            consumer = KafkaConsumer(config, self.logger)

            # Optional: connectivity test using list_topics
            metadata = consumer.consumer.list_topics(timeout=30)
            if not metadata.topics:
                self.logger.warning("No topics found during metadata fetch.")
            else:
                self.logger.debug("Kafka consumer metadata fetch successful.")

            return consumer
        except KafkaException as e:
            self.logger.error(f"Failed to create Kafka consumer: {e}")
            raise