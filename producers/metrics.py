import json
import logging
import time
from datetime import datetime
from kafka import KafkaProducer
from typing import Dict, Any

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logging.Formatter.converter = time.gmtime


class MetricsProducer:
    def __init__(self, bootstrap_servers: list,
                 metrics_topic: str,
                 dlq_topic: str):
        """
        Initialize metrics producer
        
        Args:
            bootstrap_servers: List of Kafka broker addresses
            metrics_topic: Topic to produce metrics to
            dlq_topic: Dead letter queue topic
        """
        self.logger = logging.getLogger(__name__)
        self.metrics_topic = metrics_topic
        self.dlq_topic = dlq_topic
        
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda x: json.dumps(x).encode('utf-8'),
        )

    def publish_metrics(self, metrics: Dict[str, Any]):
        """Publish metrics message to Kafka topic"""
        try:
            self.producer.send(
                self.metrics_topic,
                value=metrics
            )
            self.producer.flush()
        except Exception as e:
            self.logger.error(f"Error publishing metrics: {str(e)}")
            self.send_to_dlq(
                self.metrics_topic,
                metrics,
                str(e),
                metrics.get('processed_at_utc', datetime.utcnow().isoformat()))

    def send_to_dlq(self, topic_name: str, message: Dict[str, Any], error: str, processed_at_utc: str):
        """Send failed message to dead letter queue"""
        dlq_message = {
            'topic_name': topic_name,
            'message': message,
            'error': error,
            'processed_at_utc': processed_at_utc,
        }

        try:
            self.producer.send(
                self.dlq_topic,
                value=dlq_message
            )
            self.producer.flush()
        except Exception as e:
            self.logger.error(f"Error sending to DLQ: {str(e)}")

    def stop(self):
        """Close producer connection"""
        self.producer.close()
