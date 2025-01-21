import argparse
from kafka import KafkaConsumer

"""
A basic Kafka consumer that just prints messages.
"""


def consume_messages(topic, bootstrap_servers):
    try:
        # Create a Kafka consumer
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=[bootstrap_servers],
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id='my-consumer-group',
            value_deserializer=lambda x: x.decode('utf-8')
        )

        print(f"Connected to Kafka. Listening to topic: {topic}")

        # Consume messages
        for message in consumer:
            print(f"Received message: {message.value}")

    except Exception as e:
        print(f"An error occurred: {e}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Kafka Consumer")
    parser.add_argument("--topic",
                        help="The Kafka topic to listen to")
    parser.add_argument("--bootstrap_servers",
                        default="localhost:29092",
                        help="Comma-separated list of Kafka bootstrap servers")
    args = parser.parse_args()

    consume_messages(args.topic, args.bootstrap_servers)
