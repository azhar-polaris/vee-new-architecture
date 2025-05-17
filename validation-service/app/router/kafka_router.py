import time
from fastapi import APIRouter, HTTPException # type: ignore
from utility_pkg import KafkaProducer
from confluent_kafka import KafkaException # type: ignore

router = APIRouter(prefix="/kafka", tags=["Kafka"])

kafka_producer_config = {
    "kafka_bootstrap_servers": "localhost:9092",
}

@router.post("/test_kafka")
def test_kafka():
    try:
    
        producer = KafkaProducer(kafka_producer_config)
        messages = [
            f"Hello Kafka {i}".encode("utf-8")
            for i in range(10000)
        ]
        producer.produce_messages_in_batch("test-topic", messages)

        producer.flush(10)
        time.sleep(5)
        return { "test": "successful", "result": "All messages produced!" }
    except KafkaException as ke:
        # Specific Kafka error
        raise HTTPException(500, f"Kafka error: {str(ke)}")
    except Exception as e:
        # General fallback
        raise HTTPException(500, f"Unexpected error: {str(e)}")