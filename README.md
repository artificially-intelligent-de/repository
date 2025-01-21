# Functionality
## Consumers
### UserLoginConsumer
- Consumes the provided `user-login` topic. 
- Generates metrics as described [below](#list-of-metrics-generated), on to a `metric-event` topic.
- Puts invalid (received) messages on to a `dead-letter-queue` topic.

### `printer.py`
- Prints messages for a provided topic.
- Can be used for exploration and/or debugging.

## Producers
### MetricsProducer
- Produces messages containing metrics on to a `metric-event` topic.

## Integration
The `UserLoginConsumer` consumer and `MetricsProducer` producer are intended to be used as a pair.
The `metrics_runner.py` scripts ties them together as a single entity/application.

## List of metrics generated
The `UserLoginConsumer` consumer derives a few metrics and aggregates messages from the `user-login` topic. These metrics serve both real-time monitoring and analytical purposes. \
All the **metrics are calculated per processing time window** (defaults to 5 minutes) in a sliding window fashion. 

| Metric name            | Definition                                         | Suitable for continuous use or further aggregation | 
|------------------------|----------------------------------------------------|----------------------------------------------------|
| `active_users`         | Number of distinct users messages were received from | Real-time monitoring                               |
| `unique_ips`           | Number of distinct IP addresses messages were received from | Real-time monitoring                               |
| `processing_rate`      | Rate of message processing by a consumer           | Real-time monitoring                               |
| `error_rate`           | Number of invalid messages received                | Dual-purpose                                       |
| `device_distribution`  | Distribution of messages as per `device_type`      | Dual-purpose                                       |
| `version_distribution` | Distribution of messages as per `app_version`      | Dual-purpose                                       |
| `location_distribution` | Distribution of messages as per `locale`           | Dual-purpose                                       |
| `total_messages` | Total number of messages received by a consumer    | Dual-purpose                                       |
| `total_errors` | Total number of invalid messages received by a consumer | Dual-purpose                                               |

# Usage
### Environment Setup
```bash
pip install -r requirements.txt
```

### Infrastructure Initialization
```bash
docker compose up
```

### Running consumer-producer pair
```bash
python metrics_runner.py
```

### Message Monitoring
```bash
python printer.py --topic <topic-name>
```

# Message Schemas
### Input schema
Message from the `user-login` topic, as produced by the provided data generator.
```json
{
  "user_id": "ac853026-90a7-4c4d-9aad-98fc851f97a7",
  "app_version": "2.3.0",
  "ip": "157.156.46.154",
  "locale": "LA",
  "device_id": "cf654cc7-2e68-463c-b0f4-7d21b068d687",
  "timestamp": 1736653976,
  "device_type": "android"
}
```

### Output schema
Message from the `metric-event` topic, as produced by the consumer introduced. 
```json
{
  "timestamp": "2025-01-21T00:02:06.015516", 
  "window_start": "2025-01-21T00:01:43.351083", 
  "window_size": 43, 
  "active_users": 43, 
  "unique_ips": 43, 
  "processing_rate": 1.95, 
  "error_rate": 4.65, 
  "device_distribution": {"android": 22, "iOS": 21}, 
  "version_distribution": {"2.3.0": 43}, 
  "location_distribution": 
   {
    "GA": 1,
    "MT": 1,
    "MS": 1,
    "HI": 1,
    "CT": 1,
    "ID": 1,
    "ME": 2,
    "IN": 2,
    "LA": 1,
    "AZ": 2,
    "MA": 1,
    "UT": 1,
    "PA": 2,
    "VT": 1,
    "SC": 1,
    "IL": 1,
    "TX": 3,
    "RI": 1,
    "FL": 1,
    "KY": 2,
    "NY": 1,
    "VA": 1,
    "WA": 2,
    "WI": 1,
    "NE": 1,
    "KS": 1,
    "SD": 1,
    "AL": 1,
    "MO": 1,
    "AR": 1,
    "IA": 1,
    "OH": 3,
    "OR": 1
  },
  "total_messages": 43, 
  "total_errors": 2}
```

### Error schema
Message from the `dead-letter-queue` topic, as produced by the consumer introduced.
```json
{
  "topic_name": "user-login",
  "message": {
    "user_id": "9f7ccb91-6cee-49dc-ad0b-652b890df32b",
    "app_version": "2.3.0",
    "ip": "155.175.73.138",
    "locale": "VA",
    "device_id": "ff7eb2d0-2447-46eb-ae87-67a926fec298",
    "timestamp": 1737421077
  },
  "error": "Missing required fields: ['device_type']",
  "processed_at_utc": "2025-01-21T00:57:57.632598"
}
```

# Technical documentation and improvements
### Scaling consumers and producers
Currently, there's only one instance of Kafka consumer and producer pair that consumes the input `user-logic` topic and aggregates the data. \
Based of various factors such as volume of messages, and latency of the pair, it may make sense to introduce a consumer group, containing multiple consumer instances, each reading from one (or more) partitions. \
Along those lines, the `metric-event` topic can be partitioned, and there may be multiple instances of producers.

### Fault-tolerance and topic partitioning
Currently, the Kafka cluster consists of a single broker. Introducing multiple brokers would lead to better load distribution. \
Once there are multiple brokers, increasing `replication-factor` (to more than one) would lead to better resiliency against broker failures.

### Batching messages 
Currently, the `MetricsProducer` producer produces (and flushes) one message at a time, for both, `metric-event` and `dead-letter-queue` topic. Depending on the volume, it may make sense to batch-produce these, instead. 

# Production deployment guidelines
In general, using infrastructure as code tool like Terraform for infrastructure provisioning and maintenance is ideal. \
Dockerizing code into a Docker image is a prerequisite in that case.

### Kafka cluster
Deploying a Kafka cluster with at least three brokers and replication factor as two–providing tolerance of up to one broker failure would be a good starting point.

### For consumers and producers
Introducing a Horizontal Pod Autoscaler (HPA) with a set range of minimum and maximum number of consumers (and producers) would be a good idea. Setting `minReplicas` to 1 can help prevent unused resources though frequent partition assignment changes may introduce processing overhead. 

### Monitoring
#### Infrastructure monitoring
We'd need to introduce a logging and metrics pipeline consisting of ingestion, parsing and monitoring for both, the Kafka cluster and Kubernetes. \
Tools such as fluentd and OTEL SDK can be used for ingestion. Elasticsearch, Grafana, Prometheus are examples of storage solutions. \
Once available, various threshold based metrics monitors can be introduced.

#### Code-level monitoring
Given the use of logging, logs can be ingested and monitored for application-level errors. Depending on criticality, monitoring may happen manually on an ad-hoc basis, or alerts may be set for any logs of `ERROR` (or above) level.    
