from kafka.admin import KafkaAdminClient, NewTopic

# Set up the Kafka admin client
admin_client = KafkaAdminClient(
    bootstrap_servers="localhost:9092",  # Kafka broker
    client_id='my_admin_client'
)

# Define topic details
topic_name = "python_topic"
num_partitions = 1  # Number of partitions for the topic
replication_factor = 1  # Replication factor (1 for single broker setup)

# Create a NewTopic object
topic = NewTopic(
    name=topic_name,
    num_partitions=num_partitions,
    replication_factor=replication_factor
)

# Try to create the topic
try:
    admin_client.create_topics(new_topics=[topic], validate_only=False)
    print(f"Topic '{topic_name}' created successfully.")
except Exception as e:
    print(f"Failed to create topic '{topic_name}': {e}")
finally:
    # Close the admin client connection
    admin_client.close()






