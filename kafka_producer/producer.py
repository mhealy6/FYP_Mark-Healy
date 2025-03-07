
from kafka import KafkaProducer
import subprocess
import json
import time
import psutil
import logging
import socket

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

KAFKA_BROKER = "kafka:9092"
TOPIC = "metrics"
RETRY_INTERVAL = 5
MAX_RETRIES = 10

LOCAL_IP = "192.168.0.189"

def get_local_ip():
    """Returns the local network IP address of the machine."""
    try:
        # Get local IP that is used to communicate outside
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))  # Google's DNS to determine the external interface
        local_ip = s.getsockname()[0]
        s.close()
        return local_ip
    except Exception as e:
        logger.error(f"❌ Failed to get local IP: {e}")
        return LOCAL_IP  # Fallback to manually set IP


def wait_for_kafka():
    """Waits for Kafka to be available before proceeding."""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            logger.info(f"🔄 Attempting Kafka connection (Attempt {attempt}/{MAX_RETRIES})...")
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                retries=5,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            )
            logger.info("✅ Kafka Producer Connected Successfully")
            return producer
        except Exception as e:
            logger.error(f"❌ Kafka connection failed: {e}")
            time.sleep(RETRY_INTERVAL)

    logger.critical("❌ Kafka is not available after multiple retries. Exiting.")
    exit(1)

def start_iperf_server():
    """Starts the iperf3 server in the background if not already running."""
    try:
        # Check if iperf3 server is already running
        result = subprocess.run(["pgrep", "iperf3"], capture_output=True, text=True)
        if result.stdout.strip():
            logger.info("✅ iperf3 server is already running.")
            return  # Exit function if already running

        subprocess.Popen(["iperf3", "-s"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        logger.info("🚀 iperf3 server started successfully.")
    except Exception as e:
        logger.error(f"❌ Failed to start iperf3 server: {e}")

def get_system_metrics():
    """Fetches system metrics using psutil."""
    cpu_usage_per_core = psutil.cpu_percent(interval=1, percpu=True)  # List of per-core CPU usage
    memory_usage = psutil.virtual_memory().percent  # Memory usage %
    process_count = len(psutil.pids())  # Total number of processes

    return {
        "measurement": "system_metrics",
        "cpu_usage_per_core": cpu_usage_per_core,
        "memory_usage": memory_usage,
        "process_count": process_count,
        "timestamp": int(time.time_ns()),  # Nanoseconds for accurate timestamps
    }


def get_iperf_metrics():
    """Runs iperf3 test and extracts network throughput using the correct local network IP."""
    try:
        local_ip = get_local_ip()  # Get and log the IP before every test
        logger.info(f"🌍 Using IP {local_ip} for iperf3 test")  

        result = subprocess.run(["iperf3", "-c", local_ip, "-J"], capture_output=True, text=True)
        iperf_data = json.loads(result.stdout)

        # Extract sum_sent and sum_received
        sum_sent = iperf_data.get("end", {}).get("sum_sent", {}).get("bits_per_second", None)
        sum_received = iperf_data.get("end", {}).get("sum_received", {}).get("bits_per_second", None)

        if sum_sent is None or sum_received is None:
            logger.warning("⚠️ iperf3 test did not return expected throughput values.")
            return None

        # Convert to Mbps
        upload_speed = round(sum_sent / 1_000_000, 2)
        download_speed = round(sum_received / 1_000_000, 2)

        # Log raw values
        logger.info(f"📊 Network Throughput (IP: {local_ip}) - Upload: {upload_speed} Mbps, Download: {download_speed} Mbps")

        return {
            "measurement": "network_metrics",
            "upload_speed": upload_speed,
            "download_speed": download_speed,
            "timestamp": int(time.time_ns()),  # ✅ Convert to nanoseconds
        }
    except Exception as e:
        logger.error(f"❌ Error running iperf3: {e}")
        return None

def main():
    producer = wait_for_kafka()
    start_iperf_server()

    while True:
        try:
            system_metrics = get_system_metrics()
            iperf_metrics = get_iperf_metrics()

            producer.send(TOPIC, value=system_metrics)
            logger.info(f"📤 Sent System Metrics: {system_metrics}")

            if iperf_metrics:
                producer.send(TOPIC, value=iperf_metrics)
                logger.info(f"📤 Sent Network Metrics: {iperf_metrics}")
            else:
                logger.warning("⚠️ No valid iperf3 data collected. Skipping network metrics.")

        except Exception as e:
            logger.error(f"❌ Error in producer loop: {e}")

        time.sleep(5)

if __name__ == "__main__":
    main()
