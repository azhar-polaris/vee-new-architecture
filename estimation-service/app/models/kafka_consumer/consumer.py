import logging
import threading
import time
from typing import List, Optional
from confluent_kafka import Consumer, KafkaException, TopicPartition # type: ignore


class KafkaConsumer:
    def __init__(self, config: dict, logger: Optional[logging.Logger] = None):
        self.logger = logger or logging.getLogger("KafkaConsumer")
        try:
            self.consumer = Consumer(config)
        except KafkaException as e:
            self.logger.error(f"Failed to create Kafka consumer: {e}")
            raise

    def subscribe(self, topics: List[str]):
        try:
            self.consumer.subscribe(topics)
            self.logger.info(f"Subscribed to topics: {topics}")
        except KafkaException as e:
            self.logger.error(f"Error subscribing to topics {topics}: {e}")
            raise

    def poll(self, cancel_event: threading.Event, timeout_ms: int = 100):
        while not cancel_event.is_set():
            msg = self.consumer.poll(timeout_ms / 1000.0)
            if msg is None:
                continue
            if msg.error():
                raise KafkaException(msg.error())
            return msg

        raise TimeoutError("Polling cancelled via cancel_event.")

    def poll_batch(
        self,
        batch_size: int,
        max_wait_ms: int,
        topic_names: List[str],
        cancel_event: Optional[threading.Event] = None,
    ) -> List:
        messages = []
        end_time = time.time() + (max_wait_ms / 1000.0)

        while len(messages) < batch_size and time.time() < end_time:
            if cancel_event and cancel_event.is_set():
                break

            msg = self.consumer.poll(0.1)
            if msg is None:
                continue
            if msg.error():
                self.logger.warning(f"Kafka error while polling: {msg.error()}")
                continue

            print(f"Received message: {msg.value().decode()}")
            messages.append(msg)

        if messages:
            self.logger.debug(f"Received {len(messages)} messages on topics {topic_names}")
        return messages

    def commit_sync(self, msg):
        if not msg:
            return
        
        """
            Synchronously commit a single message's offset.
        """
        offset = TopicPartition(msg.topic(), msg.partition(), msg.offset() + 1)
        self.consumer.commit(offsets=[offset], asynchronous=False)

    def commit_sync_batch(self, messages: List):
        if not messages:
            return

        offsets = [
            TopicPartition(
                topic=msg.topic(),
                partition=msg.partition(),
                offset=msg.offset() + 1
            )
            for msg in messages
        ]
        self.consumer.commit(offsets=offsets, asynchronous=False)

    def commit_async(self):
        def async_commit():
            try:
                self.consumer.commit(asynchronous=True)
            except KafkaException as e:
                self.logger.error(f"Kafka async commit error: {e}")

        threading.Thread(target=async_commit, daemon=True).start()

    def close(self):
        self.logger.info("Closing Kafka consumer.")
        self.consumer.close()