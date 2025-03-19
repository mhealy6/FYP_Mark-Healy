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
INFLUX_URL = f"http://{INFLUX_HOSTNAME}:{INFLUX_PORT}"
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


def write_to_influxdb(data):
    """Writes Kafka data into InfluxDB."""
    try:
        logger.info(f"🚀 [DEBUG] Writing to InfluxDB: {json.dumps(data, indent=2)}")
        # Convert np.float64 to float (Fix JSON serialization issue)
        for key, value in data.items():
            if isinstance(value, list):
                data[key] = [float(v) if isinstance(v, (int, float)) else v for v in value]
            elif isinstance(value, (int, float)):
                data[key] = float(value)

        point = Point(data["measurement"]).tag("source", "kafka_consumer")
        
        for key, value in data.items():
            if key not in ["measurement", "timestamp"]:
                point = point.field(key, value)

        influx_timestamp = int(data["timestamp"]) if "timestamp" in data else int(time.time_ns())
        point = point.time(influx_timestamp)

        write_api.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=point)
        logger.info(f"✅ Successfully written to InfluxDB: {data}")

    except Exception as e:
        logger.error(f"❌ Error writing to InfluxDB: {e}")


def process_message(message):
    """Processes and writes Kafka messages to InfluxDB."""
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

        # ✅ Flatten nested fields before writing
        fields = {}
        for key, value in message_json.items():
            if key == "cpu_usage_per_core" and isinstance(value, list):
                total_cpu_usage = sum(value) / len(value) if value else 0
                fields["cpu_usage"] = float(total_cpu_usage)
                for i, core_usage in enumerate(value):
                    fields[f"cpu_core_{i}"] = float(core_usage)
            elif key == "load_average" and isinstance(value, list):  # ✅ Flatten list fields
                for i, avg in enumerate(value):
                    fields[f"load_average_{i}"] = float(avg)
            elif isinstance(value, dict):  # ✅ Flatten dict fields
                for sub_key, sub_value in value.items():
                    fields[f"{key}_{sub_key}"] = float(sub_value) if isinstance(sub_value, (int, float)) else str(sub_value)
            elif key not in ["measurement", "timestamp"]:
                try:
                    fields[key] = float(value) if isinstance(value, (int, float)) else value
                except ValueError:
                    logger.warning(f"⚠ Skipping field {key}: Unable to convert {value} to float")

        logger.info(f"📊 Processed Data: measurement={measurement}, fields={fields}, timestamp={influx_timestamp}")

        # ✅ Send to InfluxDB after flattening
        point = Point(measurement).tag("source", "kafka_consumer")
        for key, value in fields.items():
            point = point.field(key, value)

        point = point.time(influx_timestamp)

        write_api.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=point)
        logger.info(f"✅ Successfully written to InfluxDB: {measurement}")

    except Exception as e:
        logger.error(f"❌ Error processing message: {e}", exc_info=True)

consumer = wait_for_kafka()
influx_client, write_api = connect_influxdb()

# ✅ Kafka Consumer Loop
for message in consumer:
    process_message(message)
