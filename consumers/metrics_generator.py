import json
import logging
import signal
import sys
import time
from collections import defaultdict
from datetime import datetime, timedelta
from kafka import KafkaConsumer, KafkaProducer
from typing import Dict, List, Any

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logging.Formatter.converter = time.gmtime


class MetricsGenerator:
    def __init__(self, bootstrap_servers: List[str],
                 input_topic: str,
                 metrics_topic: str,
                 dlq_topic: str,
                 window_size: int = 60):  # 1-minute window
        """
        Initialize with Kafka configuration

        Args:
            bootstrap_servers: List of Kafka broker addresses
            input_topic: Kafka topic to consume raw events
            metrics_topic: Kafka topic to produce metrics
            window_size: Size of tumbling window in seconds
        """
        self.logger = logging.getLogger(__name__)
        self.window_size = window_size
        self.running = False
        self.metrics_topic = metrics_topic
        self.dlq_topic = dlq_topic

        # Window management
        self.current_window_start = None
        self.current_window_messages = []

        # Initialize Kafka consumer
        self.consumer = KafkaConsumer(
            input_topic,
            bootstrap_servers=bootstrap_servers,
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            auto_offset_reset='latest',
            enable_auto_commit=True,
        )

        # Initialize downstream Kafka producer
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda x: json.dumps(x).encode('utf-8'),
        )

        # Initialize metrics storage
        self.initialize_metrics()

        # Register signal handler
        signal.signal(signal.SIGINT, self._signal_handler)

    def initialize_metrics(self):
        """Initialize metrics containers for current window"""
        self.metrics = {
            'active_users': set(),
            'device_counts': defaultdict(int),
            'version_counts': {},
            'location_counts': {},
            'errors': 0,
            'messages_processed': 0,
            'processing_rate': 0.0,
            'unique_ips': set(),
        }

    def process_message(self, message: Dict[str, Any]) -> None:
        """Process a single message and update metrics"""
        try:
            # Validate required fields
            required_fields = ['timestamp', 'user_id', 'ip', 'device_type', 'app_version', 'locale']
            missing_fields = [field for field in required_fields
                              if field not in message]

            if missing_fields:
                raise ValueError(f"Missing required fields: {missing_fields}")

            # Convert timestamp
            try:
                timestamp = datetime.fromtimestamp(message['timestamp'])
            except (ValueError, TypeError) as e:
                raise ValueError(f"Invalid timestamp format: {str(e)}")

            # Check if we need to start a new window
            current_time = datetime.utcnow()
            if self.current_window_start is None:
                self.current_window_start = current_time
            else:
                time_elapsed = (current_time - self.current_window_start).total_seconds()
                # print(f"time_elapsed: {time_elapsed}, type(time_elapsed): {type(time_elapsed)}")
                # print(f"window_size: {self.window_size}, type(window_size): {type(self.window_size)}")
                # Check if we've exceeded the window size
                if time_elapsed >= self.window_size:
                    self.calculate_and_emit_metrics()

                    # Start new window
                    self.current_window_start = current_time
                    self.current_window_messages = []
                    self.initialize_metrics()

            # Update metrics
            self.metrics['active_users'].add(message['user_id'])
            self.metrics['unique_ips'].add(message['ip'])

            # Update device counts
            device_type = message['device_type']
            self.metrics['device_counts'][device_type] += 1

            # Update version counts
            version = message['app_version']
            self.metrics['version_counts'][version] = \
                self.metrics['version_counts'].get(version, 0) + 1

            # Update location counts
            locale = message['locale']
            self.metrics['location_counts'][locale] = \
                self.metrics['location_counts'].get(locale, 0) + 1

            # Add to current window
            processed_message = {
                'processed_at': current_time,
                **message,
            }
            self.current_window_messages.append(processed_message)

            # Update processing metrics
            self.metrics['messages_processed'] += 1

        except Exception as e:
            self.metrics['errors'] += 1
            self.send_to_dlq(message, str(e))

    def calculate_and_emit_metrics(self):
        """Calculate and emit metrics for the current window"""
        if not self.current_window_messages:
            return

        try:
            # Calculate processing rate (messages per second)
            current_time = datetime.utcnow()
            window_duration = int((current_time - self.current_window_start).total_seconds())
            processing_rate = len(self.current_window_messages) / max(1, window_duration)

            # Calculate error rate
            error_rate = self.metrics['errors'] / max(1, self.metrics['messages_processed'])

            # Prepare metrics message
            metrics_message = {
                'timestamp': datetime.utcnow().isoformat(),
                'window_start': self.current_window_start.isoformat(),
                'window_size': len(self.current_window_messages),
                'active_users': len(self.metrics['active_users']),
                'unique_ips': len(self.metrics['unique_ips']),
                'processing_rate': round(processing_rate, 2),
                'error_rate': round(error_rate * 100, 2),
                'device_distribution': self.metrics['device_counts'],
                'version_distribution': self.metrics['version_counts'],
                'location_distribution': self.metrics['location_counts'],
                'total_messages': self.metrics['messages_processed'],
                'total_errors': self.metrics['errors'],
            }

            # Produce to Kafka
            self.producer.send(
                self.metrics_topic,
                value=metrics_message,
            )
            # Ensure message is sent
            self.producer.flush()

        except Exception as e:
            self.logger.error(f"Error calculating metrics: {str(e)}.")

    def send_to_dlq(self, message: Dict[str, Any], error: str):
        """Send failed message to dead letter queue with error details"""
        dlq_message = {
            'original_message': message,
            'error': str(error),
            'processed_at_utc': datetime.utcnow().isoformat(),
            'topic_received_from': self.metrics_topic,
        }

        try:
            self.producer.send(
                self.dlq_topic,
                value=dlq_message
            )
            self.producer.flush()
        except Exception as e:
            self.logger.error(f"Error sending to DLQ: {str(e)}")

    def start(self):
        self.running = True

        try:
            self.logger.info("Started metrics generator.")
            for message in self.consumer:
                if not self.running:
                    self.logger.info("Stopping metrics generator...")
                    break

                self.process_message(message.value)

        except KeyboardInterrupt:
            self.stop()

    def stop(self):
        """Stop processing"""
        self.running = False
        # Emit final metrics before shutting down
        self.calculate_and_emit_metrics()
        self.consumer.close()
        self.producer.close()
        self.logger.info("Stopped metrics generator.")

    def _signal_handler(self, sig, frame):
        self.logger.info("\nStopping generator...")
        self.stop()
        sys.exit(0)


# Example usage
if __name__ == "__main__":
    # Configuration
    KAFKA_SERVERS = ['localhost:29092']
    INPUT_TOPIC = 'user-login'
    METRICS_TOPIC = 'metric-event'
    DLQ_TOPIC = 'metric-event-dlq'
    WINDOW_SIZE = 15  # TODO change to 60 secs

    # Initialize metrics generator
    metrics_generator = MetricsGenerator(
        bootstrap_servers=KAFKA_SERVERS,
        input_topic=INPUT_TOPIC,
        metrics_topic=METRICS_TOPIC,
        dlq_topic=DLQ_TOPIC,
        window_size=WINDOW_SIZE,
    )

    # Start processing
    metrics_generator.start()
