from kafka import KafkaProducer
import subprocess
import json
import time
import logging
import socket
import speedtest
import threading
import os

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
        result = subprocess.run(["pgrep", "iperf3"], capture_output=True, text=True)
        if result.stdout.strip():
            logger.info("✅ iperf3 server is already running.")
            return

        subprocess.Popen(["iperf3", "-s"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        logger.info("🚀 iperf3 server started successfully.")
    except Exception as e:
        logger.error(f"❌ Failed to start iperf3 server: {e}")


def get_disk_usage():
    """Fetches disk usage with df -B1 (bytes) for precise values."""
    result = subprocess.run(["df", "-B1", "/"], capture_output=True, text=True)
    lines = result.stdout.split("\n")[1].split()

    total_disk = round(int(lines[1]) / (1024**3), 2)  # Convert to GB
    used_disk = round(int(lines[2]) / (1024**3), 2)
    free_disk = round(int(lines[3]) / (1024**3), 2)

    return total_disk, used_disk, free_disk


def get_process_count():
    """Reads total number of processes from /proc/stat."""
    with open("/proc/stat", "r") as f:
        lines = f.readlines()

    for line in lines:
        if line.startswith("procs_running"):
            return int(line.split()[1])

    return 0  # Default in case of failure


def get_cpu_usage_per_core():
    """Reads CPU usage per core from /proc/stat"""
    with open("/proc/stat", "r") as f:
        lines = f.readlines()

    cpu_data = [line.split() for line in lines if line.startswith("cpu")]
    cpu_usage = []

    for cpu in cpu_data[1:]:  # Skip the first line (aggregated CPU)
        values = list(map(int, cpu[1:]))  # Ignore the first 'cpuX' label
        total_time = sum(values)
        idle_time = values[3]

        cpu_usage.append((total_time, idle_time))  # Save total and idle

    return cpu_usage


def calculate_cpu_percent(interval=1):
    """Calculates CPU usage percentage per core over a given interval."""
    start = get_cpu_usage_per_core()
    time.sleep(interval)
    end = get_cpu_usage_per_core()

    cpu_percents = []
    for (start_total, start_idle), (end_total, end_idle) in zip(start, end):
        total_diff = end_total - start_total
        idle_diff = end_idle - start_idle
        usage = (1 - (idle_diff / total_diff)) * 100
        cpu_percents.append(round(usage, 2))

    return cpu_percents  # Returns a list of per-core usage percentages


def get_memory_usage():
    """Reads memory usage from /proc/meminfo and computes usage like htop."""
    with open("/proc/meminfo", "r") as f:
        meminfo = f.readlines()

    meminfo_dict = {}
    for line in meminfo:
        key, value = line.split(":")
        meminfo_dict[key.strip()] = int(value.strip().split()[0])  # Convert KB to int

    # Fetch required values from /proc/meminfo
    total_memory = meminfo_dict["MemTotal"]
    free_memory = meminfo_dict["MemFree"]
    buffers = meminfo_dict["Buffers"]
    cached = meminfo_dict["Cached"]

    # Match htop formula: Used memory excludes buffers & cached
    used_memory = total_memory - free_memory - buffers - cached

    memory_usage_percent = (used_memory / total_memory) * 100
    return round(memory_usage_percent, 2)  # ✅ Corrected memory usage



def get_iperf_metrics():
    """Runs iperf3 test and extracts network throughput using the correct Docker IP."""
    try:
        local_ip = get_local_ip()
        logger.info(f"🌍 Using IP {local_ip} for iperf3 test")  

        # ✅ Ensure iperf3 server is running before testing
        iperf_server_check = subprocess.run(["pgrep", "iperf3"], capture_output=True, text=True)
        if not iperf_server_check.stdout.strip():
            logger.warning("⚠️ iperf3 server is NOT running! Restarting...")
            subprocess.Popen(["iperf3", "-s", "-B", "0.0.0.0"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            time.sleep(2)  # Wait for server to start

        # ✅ Run iperf3 test with bidirectional traffic (upload + download)
        result = subprocess.run(["iperf3", "-c", local_ip, "-J", "-R"], capture_output=True, text=True)
        iperf_data = json.loads(result.stdout)

        sum_sent = iperf_data.get("end", {}).get("sum_sent", {}).get("bits_per_second", None)
        sum_received = iperf_data.get("end", {}).get("sum_received", {}).get("bits_per_second", None)

        if sum_sent is None or sum_received is None:
            logger.warning("⚠️ iperf3 test did not return expected throughput values.")
            return None

        upload_speed = round(sum_sent / 1_000_000, 2)  # Convert to Mbps
        download_speed = round(sum_received / 1_000_000, 2)

        logger.info(f"📊 Docker Network - Upload: {upload_speed} Mbps, Download: {download_speed} Mbps")

        return {
            "measurement": "network_metrics",
            "source": "docker",
            "upload_speed": upload_speed,
            "download_speed": download_speed,
            "timestamp": int(time.time_ns()),
        }
    except Exception as e:
        logger.error(f"❌ Error running iperf3: {e}")
        return None



def get_network_speed():
    """Runs speedtest in a separate thread to avoid blocking."""
    try:
        st = speedtest.Speedtest()
        st.get_best_server()
        upload_speed = round(st.upload() / 1_000_000, 2)
        download_speed = round(st.download() / 1_000_000, 2)

        return {
            "measurement": "local_network",
            "source": "local",
            "upload_speed": upload_speed,
            "download_speed": download_speed,
            "timestamp": int(time.time_ns()),
        }
    except Exception as e:
        logger.error(f"❌ Error running speedtest: {e}")
        return None


def send_network_metrics(producer):
    """Handles sending network metrics asynchronously to avoid blocking."""
    while True:
        try:
            network_metrics = get_network_speed()
            if network_metrics:
                producer.send(TOPIC, value=network_metrics)
                producer.flush()
                logger.info(f"📤 [SENT] Local Network Speed Metrics: {network_metrics}")
        except Exception as e:
            logger.error(f"❌ Error in network speedtest: {e}")
        time.sleep(10)  # Run every 30 seconds


def main():
    producer = wait_for_kafka()
    start_iperf_server()

    # ✅ Start Speedtest in a separate thread (every 10 seconds)
    threading.Thread(target=send_network_metrics, args=(producer,), daemon=True).start()

    while True:
        try:
            # ✅ Collect System Metrics
            system_metrics = {
                "measurement": "system_metrics",
                "cpu_usage_per_core": calculate_cpu_percent(interval=1),
                "cpu_usage": sum(calculate_cpu_percent(interval=1)) / os.cpu_count(),
                "memory_usage": get_memory_usage(),
                "disk_total": get_disk_usage()[0],
                "disk_used": get_disk_usage()[1],
                "disk_free": get_disk_usage()[2],
                "process_count": get_process_count(),
                "timestamp": int(time.time_ns()),
            }

            producer.send(TOPIC, value=system_metrics)
            producer.flush()
            logger.info(f"📤 [SENT] System Metrics: {system_metrics}")

            # ✅ Collect Docker Network Metrics (iperf3)
            iperf_metrics = get_iperf_metrics()
            if iperf_metrics:
                producer.send(TOPIC, value=iperf_metrics)
                producer.flush()
                logger.info(f"📤 [SENT] Docker Network Metrics: {iperf_metrics}")

        except Exception as e:
            logger.error(f"❌ Error in producer loop: {e}")

        time.sleep(1)  # ✅ Send every second

if __name__ == "__main__":
    main()
