import logging
import sys
import os
import threading
from fastapi import FastAPI # type: ignore

# Add the `src/` directory to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../app')))

from router.estimation_router import router as estimation_router
from models.kafka_consumer.consumer_config import KafkaConsumerConfig
from models.kafka_consumer.consumer_factory import KafkaConsumerFactory
from models.kafka_consumer.consumer_utility import KafkaConsumerUtility

app = FastAPI(title="Estimation Service")

logger = logging.getLogger("KafkaTest")
logger.setLevel(logging.DEBUG)
logger.addHandler(logging.StreamHandler())

# Shared state for threads and cancel event
app.state.cancel_event = threading.Event()
app.state.consumer_thread = None

# Dummy message processor
def process_messages(cancel_event, messages):
    for msg in messages:
        print(f"Received message: {msg.value().decode()} from {msg.topic()}")

@app.on_event("startup")
def start_kafka_consumer():
    config = KafkaConsumerConfig(     
        consumer_group_id="test-consumer-group",
        topics=["test-topic"],
        batch_size=1000,
        max_wait_ms=150,
        num_workers=10,
        channel_buffer_size=30,
        

        # security_protocol=None,
        # bootstrap_servers="localhost:9092",
        # sasl_username=None,
        # sasl_password=None,
        # sasl_mechanism=None
    )

    factory = KafkaConsumerFactory(config, logger)
    utility = KafkaConsumerUtility(factory, logger)

    def run_consumer():
        utility.run_consumer(
            config=config,
            message_processor=process_messages,
            cancel_event=app.state.cancel_event
        )

    thread = threading.Thread(target=run_consumer, daemon=True)
    thread.start()
    app.state.consumer_thread = thread
    logger.info("Kafka consumer thread started")

@app.on_event("shutdown")
def shutdown_kafka_consumer():
    logger.info("Shutting down Kafka consumer...")
    app.state.cancel_event.set()
    if app.state.consumer_thread:
        app.state.consumer_thread.join()
    logger.info("Kafka consumer thread stopped")

@app.get("/health-check")
def root():
    return {"message": "Estimation service is running!"}

app.include_router(estimation_router)