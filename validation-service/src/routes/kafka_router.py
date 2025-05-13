from fastapi import APIRouter
from models.kafka_producer import KafkaProducerSingleton

router = APIRouter(prefix="/kafka", tags=["Kafka"])

@router.post("/test-produce")
def test_kafka_produce():
    producer = KafkaProducerSingleton.get_instance()
    producer.send_bulk_random_messages(topic="test-topic", count=5)
    return {"status": "Messages sent"}