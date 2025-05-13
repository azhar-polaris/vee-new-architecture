import sys
import os

# Add the `src/` directory to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))

from fastapi import FastAPI
from routes.validation_router import router as validation_router
from routes.kafka_router import router as kafka_router
from models.kafka_producer import KafkaProducerSingleton

KAFKA_CONFIG = {
    "kafka_bootstrap_servers": "localhost:9092",  # Update as per your Kafka setup
    "client_id": "estimation-service-producer"
}

app = FastAPI(title="Validation Service")

@app.on_event("startup")
def startup_event():
    """Initialize the Kafka producer when the app starts."""
    KafkaProducerSingleton.initialize(KAFKA_CONFIG)
    print("Kafka producer initialized.")


@app.on_event("shutdown")
def shutdown_event():
    """Flush and clean up the Kafka producer on app shutdown."""
    producer = KafkaProducerSingleton.get_instance()
    producer.flush()
    print("Kafka producer flushed and cleaned up.")


@app.get("/health-check")
def root():
    return {"message": "Validation service is running!"}

app.include_router(validation_router)
app.include_router(kafka_router)