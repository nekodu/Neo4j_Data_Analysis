from kafka import KafkaConsumer
from neo4j import GraphDatabase
import json

# Neo4j connection parameters
URI = "bolt://localhost:7687"
USER = "neo4j"
PASSWORD = "your_password"
# Kafka topic to consume messages from
KAFKA_TOPIC = "transactions"

# Create a connection to the Neo4j database
driver = GraphDatabase.driver(URI, auth=(USER, PASSWORD))

# Function to create and update the graph in Neo4j
def create_graph(tx, data):
    tx.run("""
        MERGE (sender:Customer {id: $sender_id})
        ON CREATE SET sender.age = $sender_age, sender.gender = $sender_gender
        MERGE (receiver:Customer {id: $receiver_id})
        ON CREATE SET receiver.age = $receiver_age, receiver.gender = $receiver_gender
        MERGE (sender)-[r:SENT_TO]->(receiver)
        ON CREATE SET r.total_amount = $amount, r.count = 1, r.first_timestamp = $timestamp, r.last_timestamp = $timestamp
        ON MATCH SET r.total_amount = r.total_amount + $amount, r.count = r.count + 1, r.last_timestamp = $timestamp
        WHERE r.first_timestamp > $timestamp
        SET r.first_timestamp = $timestamp
    """, sender_id=data["sender"]["id"], sender_age=data["sender"]["age"], sender_gender=data["sender"]["gender"],
        receiver_id=data["receiver"]["id"], receiver_age=data["receiver"]["age"], receiver_gender=data["receiver"]["gender"],
        amount=data["amount"], timestamp=data["timestamp"])

# Function to consume messages from Kafka and update the graph in Neo4j
def consume_kafka_messages(topic):
    # Create a Kafka consumer with deserializer for JSON messages
    consumer = KafkaConsumer(topic, bootstrap_servers="localhost:9092", value_deserializer=lambda m: json.loads(m.decode("utf-8")))
    
    # Consume messages from the Kafka topic
    for message in consumer:
        # Update the graph in Neo4j with the consumed message
        with driver.session() as session:
            session.write_transaction(create_graph, message.value)

# Main function to start consuming messages from Kafka
if __name__ == "__main__":
    consume_kafka_messages(KAFKA_TOPIC)
