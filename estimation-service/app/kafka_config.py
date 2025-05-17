import logging
from utility_pkg import KafkaConsumerConfig

logger = logging.getLogger("Kafka")
logger.setLevel(logging.INFO)
logger.addHandler(logging.StreamHandler())

def get_profile_instant_config():
    return KafkaConsumerConfig(
        bootstrap_servers="localhost:9092",
        consumer_group_id="profile-instant-group",
        topics=["test-topic"],
        batch_size=1000,
        max_wait_ms=150,
        num_workers=10,
        channel_buffer_size=30
    )

def get_blockload_config():
    return KafkaConsumerConfig(
        bootstrap_servers="localhost:9092",
        consumer_group_id="blockload-group",
        topics=["blockload-topic"],
        batch_size=500,
        max_wait_ms=150,
        num_workers=5,
        channel_buffer_size=30
    )