import signal
import sys
from consumers.user_login import UserLoginConsumer
from producers.metrics import MetricsProducer

# Consumer and producer configuration
KAFKA_SERVERS = ['localhost:29092']
INPUT_TOPIC = 'user-login'
METRICS_TOPIC = 'metric-event'
DLQ_TOPIC = 'dead-letter-queue'
WINDOW_SIZE = 300


def main():
    # Initialize producer
    producer = MetricsProducer(
        bootstrap_servers=KAFKA_SERVERS,
        metrics_topic=METRICS_TOPIC,
        dlq_topic=DLQ_TOPIC
    )

    # Initialize consumer with producer callback
    consumer = UserLoginConsumer(
        bootstrap_servers=KAFKA_SERVERS,
        input_topic=INPUT_TOPIC,
        metrics_callback=producer.publish_metrics,
        dlq_callback=producer.send_to_dlq,
        window_size=WINDOW_SIZE
    )

    # Handle shutdown gracefully
    def signal_handler(sig, frame):
        consumer.stop()
        producer.stop()
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)

    # Start processing
    consumer.start()


if __name__ == "__main__":
    main()
