from fastapi import APIRouter, HTTPException
from models.kafka_producer import KafkaProducerSingleton
from models.kafka_producer import KafkaProducerSingleton
from confluent_kafka import KafkaException # type: ignore

router = APIRouter(prefix="/kafka", tags=["Kafka"])

@router.post("/test_kafka")
def test_kafka():
    try:
        producer = KafkaProducerSingleton.get_instance()
        producer.send_bulk_random_messages(topic="test-topic", count=2000)
        return { "test": "successful" }
    except KafkaException as ke:
        # Specific Kafka error
        raise HTTPException(500, f"Kafka error: {str(ke)}")
    except Exception as e:
        # General fallback
        raise HTTPException(500, f"Unexpected error: {str(e)}")