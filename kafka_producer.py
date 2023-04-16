import json
from kafka import KafkaProducer
from data_generation import generate_random_data, Customer, Transaction

# Define the Kafka topic to send the messages
KAFKA_TOPIC = "kafka-topic"

# Function to send the generated transactions to Kafka
def send_to_kafka(producer, customers, transactions):
    for transaction in transactions:
        # Find the sender and receiver customer objects based on their IDs
        sender = next(filter(lambda c: c.id == transaction.sender_id, customers))
        receiver = next(filter(lambda c: c.id == transaction.receiver_id, customers))

        # Create a dictionary to represent the transaction data
        data = {
            "transaction_id": transaction.id,
            "sender": {
                "id": sender.id,
                "age": sender.age,
                "gender": sender.gender,
            },
            "receiver": {
                "id": receiver.id,
                "age": receiver.age,
                "gender": receiver.gender,
            },
            "amount": transaction.amount,
            "timestamp": transaction.timestamp.isoformat(),
            "currency": transaction.currency,
        }

        # Send the transaction data to the Kafka topic as a JSON-encoded string
        producer.send(KAFKA_TOPIC, json.dumps(data).encode("utf-8"))

# Main function to generate the data and send it to Kafka
if __name__ == "__main__":
    # Generate random customers and transactions
    customers, transactions = generate_random_data(10, 100)
    
    # Create a Kafka producer
    producer = KafkaProducer(bootstrap_servers="localhost:9092")
    
    # Send the generated transactions to Kafka
    send_to_kafka(producer, customers, transactions)
