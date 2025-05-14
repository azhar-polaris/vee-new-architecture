import time
from confluent_kafka import Consumer, KafkaException # type: ignore

class KafkaConsumerClass:
    def __init__(
        self,
        broker,
        group_id='python-consumer-group',
        topic='test',
        batch_size=1000,
        batch_timeout=2.0,
        auto_commit=True,
        strict_batch_size=False,
    ):
        self.broker = broker
        self.group_id = group_id
        self.topic = topic
        self.batch_size = batch_size
        self.batch_timeout = batch_timeout
        self.auto_commit = auto_commit
        self.strict_batch_size = strict_batch_size
        self.consumer = None
        self._configure_consumer()

    def _configure_consumer(self):
        conf = {
            'bootstrap.servers': self.broker,
            'group.id': self.group_id,
            'enable.auto.commit': False,  # we manage commits manually
            'auto.offset.reset': 'earliest',
        }
        self.consumer = Consumer(conf)
        self.consumer.subscribe([self.topic])

    def _poll_batch(self):
        batch = []
        start_time = time.time()

        while (time.time() - start_time) < self.batch_timeout:
            if not self.strict_batch_size and len(batch) >= self.batch_size:
                break

            msg = self.consumer.poll(0.1)

            if msg is None:
                continue
            if msg.error():
                print(f"Error: {msg.error()}")
                continue

            try:
                batch.append(msg.value().decode('utf-8'))
            except UnicodeDecodeError:
                print("Warning: failed to decode message, skipping.")

            if self.strict_batch_size and len(batch) >= self.batch_size:
                break

        if batch and len(batch) < self.batch_size:
            print(f"(Partial batch received: {len(batch)} messages)")

        return batch

    def consume_batches(self, max_batches=None):
        total_messages = 0
        batch_count = 0

        try:
            while max_batches is None or batch_count < max_batches:
                batch = self._poll_batch()

                if not batch:
                    time.sleep(0.5)
                    continue

                batch_count += 1
                total_messages += len(batch)

                print(f"\n[Batch {batch_count}] Received {len(batch)} messages. Total so far: {total_messages}")
                for idx, msg in enumerate(batch, 1):
                    print(idx, msg)

                if self.auto_commit:
                    self.consumer.commit(asynchronous=True)

        except KeyboardInterrupt:
            print(f"\nStopped. Total messages received: {total_messages}")
        except KafkaException as e:
            print(f"Kafka error: {e}")
        finally:
            self.consumer.close()