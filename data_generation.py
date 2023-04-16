import random
from datetime import datetime, timedelta

# Define the Customer class
class Customer:
    def __init__(self, id, age, gender):
        self.id = id
        self.age = age
        self.gender = gender

# Define the Transaction class
class Transaction:
    def __init__(self, id, sender_id, receiver_id, amount, timestamp, currency):
        self.id = id
        self.sender_id = sender_id
        self.receiver_id = receiver_id
        self.amount = amount
        self.timestamp = timestamp
        self.currency = currency

# Function to generate random data for customers and transactions
def generate_random_data(num_customers, num_transactions):
    # Create a list of customers with random ages and genders
    customers = [Customer(i, random.randint(18, 60), random.choice(["F", "M"])) for i in range(1, num_customers+1)]
    
    # Initialize an empty list to store the transactions
    transactions = []

    # Generate the specified number of transactions
    for i in range(1, num_transactions+1):
        # Choose a random sender from the list of customers
        sender = random.choice(customers)
        
        # Choose a random receiver from the list of customers, excluding the sender
        receiver = random.choice([c for c in customers if c.id != sender.id])
        
        # Generate a random amount, timestamp, and currency for the transaction
        amount = random.uniform(10, 500)
        timestamp = datetime.now() - timedelta(days=random.randint(1, 365))
        currency = "TRY" if random.random() < 0.9 else "USD"
        
        # Create a Transaction object and append it to the transactions list
        transaction = Transaction(i, sender.id, receiver.id, amount, timestamp, currency)
        transactions.append(transaction)

    # Return the generated customers and transactions
    return customers, transactions
