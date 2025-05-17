import asyncio
import inspect
import threading
import queue
import time
from typing import Callable, List, Optional
from confluent_kafka import Message # type: ignore
from .consumer_factory import KafkaConsumerFactory
from .consumer_config import KafkaConsumerConfig


class KafkaConsumerUtility:
    def __init__(self, consumer_factory: KafkaConsumerFactory, logger):
        self.consumer_factory = consumer_factory
        self.logger = logger
        self.total_messages_processed = 0 

    def run_consumer(
        self,
        config: KafkaConsumerConfig,
        message_processor: Callable[[threading.Event, List[Message]], None],
        cancel_event: Optional[threading.Event] = None
    ):
        cancel_event = cancel_event or threading.Event()

        consumer = self.consumer_factory.create_consumer(config.consumer_group_id)
        consumer.subscribe(config.topics)

        msg_queue = queue.Queue(maxsize=config.channel_buffer_size)

        threads = []

        # Polling Thread
        poller_thread = threading.Thread(
            target=self._poller,
            args=(consumer, msg_queue, config, cancel_event),
            name="KafkaPoller"
        )
        poller_thread.start()
        self.logger.info("Started polling thread: KafkaPoller")
        threads.append(poller_thread)

        # Worker Threads
        for i in range(config.num_workers):
            t = threading.Thread(
                target=self._worker,
                args=(consumer, msg_queue, message_processor, config, cancel_event),
                name=f"KafkaWorker-{i+1}"
            )
            t.start()
            self.logger.info(f"Started worker thread: KafkaWorker-{i+1}")
            threads.append(t)

        try:
            while not cancel_event.is_set():
                time.sleep(1)
        except KeyboardInterrupt:
            self.logger.info("Shutdown requested by user.")
            cancel_event.set()

        for t in threads:
            t.join()

        consumer.close()
        self.logger.info("Consumer closed, all threads joined.")
        self.logger.info(f"Total messages processed: {self.total_messages_processed}")

    def _poller(self, consumer, msg_queue, config, cancel_event):
        while not cancel_event.is_set():
            try:
                messages = consumer.poll_batch(
                    batch_size=config.batch_size,
                    max_wait_ms=config.max_wait_ms,
                    topic_names=config.topics,
                    cancel_event=cancel_event
                )
                if messages:
                    try:
                        msg_queue.put(messages, timeout=1)
                    except queue.Full:
                        self.logger.warning("Message queue full. Dropping batch.")
            except Exception as e:
                self.logger.error(f"Polling error: {e}")

    def _worker(self, consumer, msg_queue, message_processor, config, cancel_event):
        while not cancel_event.is_set() or not msg_queue.empty():
            try:
                messages = msg_queue.get(timeout=1)
                if messages:
                    try:
                        if inspect.iscoroutinefunction(message_processor):
                            # Run async message processor in event loop
                            asyncio.run(message_processor(cancel_event, messages))
                        else:
                            message_processor(cancel_event, messages)
                    except Exception as e:
                        self.logger.error(f"Message processing error: {e}")

                    try:
                        consumer.commit_sync_batch(messages)
                    except Exception as e:
                        self.logger.error(f"Commit failed: {e}")
            except queue.Empty:
                continue