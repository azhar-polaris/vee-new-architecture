import sys
import os
from fastapi import FastAPI
# Add the `src/` directory to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../app')))

from confluent_kafka import KafkaException # type: ignore
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
    """Initialize the Kafka producer and verify Kafka connection."""
    try:
        KafkaProducerSingleton.initialize(KAFKA_CONFIG)
        print("✅ Kafka producer initialized.")

        # ✅ Send a test message to verify Kafka is reachable
        producer = KafkaProducerSingleton.get_instance()
        producer.send_message("health-check", key="startup", value="Kafka is up")
        producer.flush()

        print("✅ Kafka connection verified: health-check message sent.")
    except KafkaException as ke:
        print("❌ KafkaException while sending test message:", ke)
        raise
    except Exception as e:
        print("❌ Failed to initialize Kafka producer:", e)
        raise


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