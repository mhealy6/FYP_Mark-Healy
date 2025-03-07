from kafka import KafkaConsumer
from influxdb_client import InfluxDBClient, Point, WriteOptions
import logging
import time
import socket
import json

# Setup Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Kafka Configuration
KAFKA_SERVICE_NAME = "kafka"
KAFKA_PORT = 9092
KAFKA_TOPIC = "metrics"

# InfluxDB Configuration
INFLUX_HOSTNAME = "influxdb"  # Use Docker container name
INFLUX_PORT = 8086
INFLUX_URL = f"http://{INFLUX_HOSTNAME}:{INFLUX_PORT}"  # Ensure it resolves
INFLUX_TOKEN = "pJD-fJlSdpGwetGSAv5rbiV53dH6rgIJ6oj4QJ2TZgbcSQjY8wZ8OLjoro53AjqqU6l2eGbwNzpdvcuVB6h6cw=="
INFLUX_ORG = "org"
INFLUX_BUCKET = "metrics"

# Retry Configuration
RETRY_INTERVAL = 10
MAX_RETRIES = 20


def get_kafka_ip():
    """Resolve Kafka's IP dynamically to avoid hardcoded values."""
    try:
        kafka_ip = socket.gethostbyname(KAFKA_SERVICE_NAME)
        logger.info(f"✅ Kafka resolved to IP: {kafka_ip}")
        return f"{kafka_ip}:{KAFKA_PORT}"
    except socket.gaierror:
        logger.error("❌ Unable to resolve Kafka hostname!")
        return None


def wait_for_kafka():
    """Keep retrying until Kafka is available."""
    kafka_broker = get_kafka_ip()
    if not kafka_broker:
        logger.error("❌ Failed to get Kafka broker IP. Exiting.")
        exit(1)

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            consumer = KafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=[kafka_broker],
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                group_id="my-consumer-group"
            )
            logger.info("✅ Kafka Consumer Connected Successfully!")
            return consumer
        except Exception as e:
            logger.error(f"⚠ Kafka connection failed (attempt {attempt}/{MAX_RETRIES}): {e}")
            time.sleep(RETRY_INTERVAL)

    logger.error("❌ Kafka not available after multiple retries. Exiting.")
    exit(1)


def connect_influxdb():
    """Attempts to connect to InfluxDB with retries."""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG, timeout=30_000)
            write_api = client.write_api()
            logger.info("✅ InfluxDB Client Connected Successfully!")
            return client, write_api
        except Exception as e:
            logger.error(f"⚠ InfluxDB Connection Failed (attempt {attempt}/{MAX_RETRIES}): {e}")
            time.sleep(RETRY_INTERVAL)

    logger.error("❌ InfluxDB not available after multiple retries. Exiting.")
    exit(1)


def write_test_data_to_influx():
    """Writes test data directly to InfluxDB to verify connectivity."""
    try:
        point = (
            Point("test_measurement")
            .tag("source", "kafka_consumer")
            .field("cpu_usage", float(42.0))  # Ensure float
            .field("memory_usage", float(75.5))  # Ensure float
            .field("process_count", float(3))  # Ensure float
            .time(int(time.time_ns()))  # ✅ Nanosecond timestamp
        )

        write_api.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=point)
        logger.info("✅ Successfully written TEST data directly to InfluxDB!")
    except Exception as e:
        logger.error(f"❌ Failed to write TEST data to InfluxDB: {e}", exc_info=True)


consumer = wait_for_kafka()
influx_client, write_api = connect_influxdb()

# ✅ Write test data before consuming Kafka messages
write_test_data_to_influx()


def process_message(message):
    try:
        decoded_value = message.value.decode('utf-8')
        logger.info(f"📩 Received from Kafka: {decoded_value}")

        message_json = json.loads(decoded_value)
        measurement = message_json.get("measurement", "unknown")
        timestamp = message_json.get("timestamp", None)

        if timestamp:
            influx_timestamp = int(timestamp) if timestamp > 10**12 else int(float(timestamp) * 1e9)
        else:
            influx_timestamp = int(time.time_ns())

        # ✅ Force all numeric values to float to prevent field type conflicts
        fields = {}
        for key, value in message_json.items():
            if key == "cpu_usage_per_core" and isinstance(value, list):
                # Compute the overall CPU usage as the average of all cores
                total_cpu_usage = sum(value) / len(value) if value else 0
                fields["cpu_usage"] = float(total_cpu_usage)  # Store total CPU usage
                # Store each core's usage as a separate field
                for i, core_usage in enumerate(value):
                    fields[f"cpu_core_{i}"] = float(core_usage)  # Store as separate fields
            elif key not in ["measurement", "timestamp"]:
                try:
                    fields[key] = float(value) if isinstance(value, (int, float)) else value
                except ValueError:
                    logger.warning(f"⚠ Skipping field {key}: Unable to convert {value} to float")

        logger.info(f"📊 Processed Data: measurement={measurement}, fields={fields}, timestamp={influx_timestamp}")

        point = Point(measurement).tag("source", "kafka_consumer")
        for key, value in fields.items():
            point = point.field(key, value)

        point = point.time(influx_timestamp)

        write_api.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=point)
        logger.info(f"✅ Successfully written to InfluxDB: {measurement}")

    except Exception as e:
        logger.error(f"❌ Error processing message: {e}", exc_info=True)


# ✅ Ensure `while True:` is correctly indented
while True:
    try:
        for message in consumer:
            process_message(message)
    except Exception as e:
        logger.error(f"❌ Error consuming message: {e}", exc_info=True)
        time.sleep(RETRY_INTERVAL)
