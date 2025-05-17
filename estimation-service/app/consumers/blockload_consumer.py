import threading
from utility_pkg import KafkaConsumerUtility, KafkaConsumerFactory
from kafka_config import get_blockload_config, logger


def process_blockload_messages(cancel_event, messages):
    for msg in messages:
        print(f"[BLOCKLOAD] {msg.value().decode()}")

def start_blockload_consumer(cancel_event):
    config = get_blockload_config()
    factory = KafkaConsumerFactory(config, logger)
    utility = KafkaConsumerUtility(factory, logger)

    def run():
        utility.run_consumer(config=config, message_processor=process_blockload_messages, cancel_event=cancel_event)

    thread = threading.Thread(target=run, daemon=True)
    thread.start()
    return thread