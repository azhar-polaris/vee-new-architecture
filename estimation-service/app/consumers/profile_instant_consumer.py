import asyncio
import threading
from utility_pkg import KafkaConsumerUtility, KafkaConsumerFactory
from kafka_config import get_profile_instant_config, logger


async def process_profile_instant_messages(cancel_event, messages):
    values = [msg.value().decode() for msg in messages]
    await asyncio.sleep(0.5)  # Simulate I/O delay
    print(f"[PROFILE INSTANT] Processed batch of {len(values)} messages")

def start_profile_instant_consumer(cancel_event):
    config = get_profile_instant_config()
    factory = KafkaConsumerFactory(config, logger)
    utility = KafkaConsumerUtility(factory, logger)

    def run():
        utility.run_consumer(config=config, message_processor=process_profile_instant_messages, cancel_event=cancel_event)

    thread = threading.Thread(target=run, daemon=True)
    thread.start()
    return thread