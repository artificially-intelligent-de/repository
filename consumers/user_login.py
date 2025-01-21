import json
import logging
import time
from collections import defaultdict
from datetime import datetime
from kafka import KafkaConsumer
from typing import Dict, Any, Callable


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logging.Formatter.converter = time.gmtime


class UserLoginConsumer:
    def __init__(self, bootstrap_servers: list,
                 input_topic: str,
                 metrics_callback: Callable,
                 dlq_callback: Callable,
                 window_size: int = 60):
        """
        Initialize user-login consumer
        
        Args:
            bootstrap_servers: List of Kafka broker addresses
            input_topic: Topic to consume raw events from
            metrics_callback: Callback function to handle computed metrics
            dlq_callback: Callback function to handle erroneous messages
            window_size: Size of tumbling window in seconds
        """
        self.logger = logging.getLogger(__name__)
        self.window_size = window_size
        self.metrics_callback = metrics_callback
        self.dlq_callback = dlq_callback
        self.running = False
        self.input_topic = input_topic
        
        # Window management
        self.current_window_start = None
        self.current_window_messages = []
        
        # Initialize consumer
        self.consumer = KafkaConsumer(
            input_topic,
            bootstrap_servers=bootstrap_servers,
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            auto_offset_reset='latest',
            enable_auto_commit=True,
        )

        self.metrics = None
        self.initialize_metrics()

    def initialize_metrics(self):
        """Initialize metrics containers for current window"""
        self.metrics = {
            'active_users': set(),
            'device_counts': defaultdict(int),
            'version_counts': defaultdict(int),
            'location_counts': defaultdict(int),
            'errors': 0,
            'messages_processed': 0,
            'unique_ips': set(),
        }

    def process_message(self, message: Dict[str, Any]) -> None:
        """Process a single message and update metrics"""
        current_time = datetime.utcnow()
        try:
            # Validate required fields
            required_fields = ['timestamp', 'user_id', 'ip', 'device_type', 'app_version', 'locale']
            missing_fields = [field for field in required_fields if field not in message]

            if missing_fields:
                raise ValueError(f"Missing required fields: {missing_fields}")

            # Check if we need to start a new window
            if self.current_window_start is None:
                self.current_window_start = current_time
            else:
                time_elapsed = (current_time - self.current_window_start).total_seconds()
                if time_elapsed >= self.window_size:
                    self.calculate_and_emit_metrics()
                    
                    # Start new window
                    self.current_window_start = current_time
                    self.current_window_messages = []
                    self.initialize_metrics()

            # Update metrics
            self.metrics['active_users'].add(message['user_id'])
            self.metrics['unique_ips'].add(message['ip'])
            self.metrics['device_counts'][message['device_type']] += 1
            
            version = message['app_version']
            self.metrics['version_counts'][version] += 1
                
            locale = message['locale']
            self.metrics['location_counts'][locale] += 1

            # Add to current window
            processed_message = {
                'processed_at': current_time,
                **message,
            }
            self.current_window_messages.append(processed_message)
            self.metrics['messages_processed'] += 1

        except Exception as e:
            self.metrics['errors'] += 1
            self.dlq_callback(self.input_topic, message, str(e), current_time.isoformat())

    def calculate_and_emit_metrics(self):
        """Calculate metrics for current window and emit via callback"""
        if not self.current_window_messages:
            return

        try:
            current_time = datetime.utcnow()
            window_duration = int((current_time - self.current_window_start).total_seconds())
            processing_rate = len(self.current_window_messages) / max(1, window_duration)
            error_rate = self.metrics['errors'] / max(1, self.metrics['messages_processed'])

            metrics_message = {
                'timestamp': datetime.utcnow().isoformat(),
                'window_start': self.current_window_start.isoformat(),
                'window_size': len(self.current_window_messages),
                'active_users': len(self.metrics['active_users']),
                'unique_ips': len(self.metrics['unique_ips']),
                'processing_rate': round(processing_rate, 2),
                'error_rate': round(error_rate * 100, 2),
                'device_distribution': dict(self.metrics['device_counts']),
                'version_distribution': self.metrics['version_counts'],
                'location_distribution': self.metrics['location_counts'],
                'total_messages': self.metrics['messages_processed'],
                'total_errors': self.metrics['errors'],
            }

            self.metrics_callback(metrics_message)

        except Exception as e:
            self.logger.error(f"Error calculating metrics: {str(e)}")

    def start(self):
        """Start consuming messages"""
        self.running = True
        try:
            self.logger.info("Started user-login consumer.")
            for message in self.consumer:
                if not self.running:
                    break
                self.process_message(message.value)
        except KeyboardInterrupt:
            self.stop()

    def stop(self):
        """Stop consuming messages"""
        self.running = False
        self.calculate_and_emit_metrics()
        self.consumer.close()
        self.logger.info("Stopped user-login consumer.")
