import sys
import os
import threading

# Add the `src/` directory to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../app')))

from fastapi import FastAPI
from routes.estimation_router import router as estimation_router
from models.kafka_consumer import KafkaConsumerClass
from confluent_kafka import KafkaException  # type: ignore

KAFKA_CONSUMER_CONFIG = {
    "broker": "localhost:9092",
    "group_id": "estimation-group",
    "topic": "test-topic",
    "batch_size": 1000,
    "batch_timeout": 2.0,
    "strict_batch_size": False
}

app = FastAPI(title="Estimation Service")

@app.on_event("startup")
def startup_event():
    global test_topic_consumer, health_check_consumer
    try:
        test_topic_consumer = KafkaConsumerClass(**KAFKA_CONSUMER_CONFIG)

        health_check_config = {**KAFKA_CONSUMER_CONFIG, "topic": 'health-check'}
        health_check_consumer = KafkaConsumerClass(**health_check_config)
        print("✅ Kafka consumers initialized.")
    except KafkaException as ke:
        print(f"❌ Kafka initialization failed: {ke}")
        raise
    except Exception as e:
        print(f"❌ Unexpected error during Kafka consumer setup: {e}")
        raise

    def run_test_topic_consumer():
        test_topic_consumer.consume_batches()

    def run_health_check_consumer():
        health_check_consumer.consume_batches()

    threading.Thread(target=run_test_topic_consumer, daemon=True).start()
    threading.Thread(target=run_health_check_consumer, daemon=True).start()

@app.on_event("shutdown")
def shutdown_event():
    print("Shutting down... Kafka consumer will exit on next poll or timeout.")


@app.get("/health-check")
def root():
    return {"message": "Estimation service is running!"}

app.include_router(estimation_router)