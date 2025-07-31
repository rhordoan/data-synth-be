import os
import json
from typing import List, Dict, Any
from kafka import KafkaProducer
from kafka.errors import KafkaError
from dotenv import load_dotenv

load_dotenv()

class KafkaProducerService:
    """
    A service class for producing messages to a Kafka topic.
    Connects to the Kafka broker specified in the environment variables.
    """
    def __init__(self):
        self.producer = None
        try:
            kafka_bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
            print(f"--- KAFKA PRODUCER ---")
            print(f"Connecting to Kafka at {kafka_bootstrap_servers}...")
            self.producer = KafkaProducer(
                bootstrap_servers=kafka_bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                api_version=(0, 10, 1) # Specify a compatible API version
            )
            print("Successfully connected to Kafka.")
        except KafkaError as e:
            print(f"ERROR: Failed to connect to Kafka: {e}")
            # In a real app, you might have more robust error handling or a retry mechanism.
            self.producer = None

    def send(self, topic: str, data: List[Dict[str, Any]]):
        if not self.producer:
            print("ERROR: Kafka producer is not available. Cannot send messages.")
            return False
            
        try:
            print(f"--- KAFKA PRODUCER ---")
            print(f"Sending {len(data)} records to Kafka topic: '{topic}'")
            for record in data:
                self.producer.send(topic, value=record)
            self.producer.flush() # Ensure all messages are sent
            print(f"Successfully sent records to '{topic}'.")
            return True
        except KafkaError as e:
            print(f"ERROR: Failed to send messages to Kafka: {e}")
            return False

    def close(self):
        if self.producer:
            print("--- KAFKA PRODUCER ---")
            print("Closing Kafka producer.")
            self.producer.close()

# Create a singleton instance to be used across the application
kafka_producer = KafkaProducerService()
