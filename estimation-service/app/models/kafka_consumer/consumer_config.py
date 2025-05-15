from dataclasses import dataclass
from typing import List, Optional

@dataclass
class KafkaConsumerConfig:
    bootstrap_servers: str
    consumer_group_id: str
    topics: List[str]
    batch_size: int
    max_wait_ms: int
    num_workers: int
    channel_buffer_size: int
    security_protocol: Optional[str] = None
    sasl_username: Optional[str] = None
    sasl_password: Optional[str] = None
    sasl_mechanism: Optional[str] = None

    def to_kafka_config(self) -> dict:
        config = {
            "bootstrap.servers": self.bootstrap_servers,
            "group.id": self.consumer_group_id,
            "auto.offset.reset": "earliest",
            "session.timeout.ms": 30000,
            "heartbeat.interval.ms": 10000,
        }

        if self.security_protocol:
            config["security.protocol"] = self.security_protocol
        if self.sasl_username:
            config["sasl.username"] = self.sasl_username
        if self.sasl_password:
            config["sasl.password"] = self.sasl_password
        if self.sasl_mechanism:
            config["sasl.mechanism"] = self.sasl_mechanism

        return config