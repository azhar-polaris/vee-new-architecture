import sys
import os
import threading

# Add the `src/` directory to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))

from fastapi import FastAPI
from routes.estimation_router import router as estimation_router
from models.kafka_consumer import KafkaConsumerSingleton

KAFKA_CONSUMER_CONFIG = {
    "broker": "localhost:9092",
    "group_id": "estimation-group",
    "topic": "test-topic",
    "batch_size": 100,
    "batch_timeout": 2.0,
    "strict_batch_size": False
}

app = FastAPI(title="Estimation Service")

consumer_thread = None

@app.on_event("startup")
def startup_event():
    KafkaConsumerSingleton.initialize(KAFKA_CONSUMER_CONFIG)
    print("Kafka consumer initialized.")

    def consume():
        consumer = KafkaConsumerSingleton.get_instance()
        consumer.consume_batches()  # blocks forever or until interrupted

    global consumer_thread
    consumer_thread = threading.Thread(target=consume, daemon=True)
    consumer_thread.start()
    print("Kafka consumer thread started.")

@app.on_event("shutdown")
def shutdown_event():
    print("Shutting down... Kafka consumer will exit on next poll or timeout.")


@app.get("/health-check")
def root():
    return {"message": "Estimation service is running!"}

app.include_router(estimation_router)