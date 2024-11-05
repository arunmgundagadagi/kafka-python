from confluent_kafka import Consumer

config = {
    'bootstrap.servers': 'localhost:9092',          # Kafka broker
    'group.id': 'another-group',
    'auto.offset.reset': 'earliest'                  # Start from beginning if no offsets are committed
}

consumer  =  Consumer(config)
consumer.subscribe(['consumertest'])

try:
    print("Waiting for messages...")
    while True:
        message = consumer.poll(1.0)  # Poll for messages, wait for up to 1 second

        if message is None:
            print("no messages found")
            break  # No message available, retry

        if message.error():
            print(f"Consumer error: {message.error()}")
            continue

        # Access the deserialized message value and key (if any)
        value = message.value()   # Value is already deserialized to a Python dictionary
        key = message.key()   # Key if available, deserialized as well if it's Avro
        print(f"Consumed message: key={key}, value={value}")
except KeyboardInterrupt:
    print("Consumer stopped.")

finally:
    # Close the consumer to commit final offsets and release resources
    consumer.close()