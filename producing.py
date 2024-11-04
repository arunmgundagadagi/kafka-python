from kafka import KafkaProducer
import time

# Set up the Kafka producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    key_serializer=lambda x: str(x).encode('utf-8'),
    value_serializer=lambda v: str(v).encode('utf-8')  # Convert values to bytes
)

# Define the topic name
topic_name = 'python_topic'
dictionary = {1:'Arun',2:'pooja'}

# Produce numbers 1 through 10
for number,name in dictionary.items():
    producer.send(topic_name, key=number,value=name)
    print(f"Produced: {number, name}")
    time.sleep(1)  # Optional: add a small delay between sends

# Flush and close the producer to ensure all messages are sent
producer.flush()
producer.close()
print("Finished producing numbers 1 to 10.")