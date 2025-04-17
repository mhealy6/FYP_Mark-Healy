from kafka import KafkaProducer
import subprocess
import json
import time
import logging
import socket
import speedtest
import threading
import os
import psutil
from statistics import mean
import collections

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

KAFKA_BROKER = "172.18.0.2:9092"
TOPIC = "metrics"
RETRY_INTERVAL = 5
MAX_RETRIES = 10
LOCAL_IP = "192.168.0.189"


def get_local_ip():
    """Returns the local network IP address of the machine."""
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        local_ip = s.getsockname()[0]
        s.close()
        return local_ip
    except Exception as e:
        logger.error(f" Failed to get local IP: {e}")
        return LOCAL_IP 


def wait_for_kafka():
    """Waits for Kafka to be available before proceeding."""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            logger.info(f" Attempting Kafka connection (Attempt {attempt}/{MAX_RETRIES})...")
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                retries=5,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            )
            logger.info("Kafka Producer Connected Successfully")
            return producer
        except Exception as e:
            logger.error(f" Kafka connection failed: {e}")
            time.sleep(RETRY_INTERVAL)

    logger.critical(" Kafka is not available after multiple retries. Exiting.")
    exit(1)

def get_cpu_usage():
    """Returns overall CPU usage using psutil (more accurate)."""
    return psutil.cpu_percent(interval=1) 

def get_memory_usage():
    """Fetches system memory usage using psutil."""
    try:
        mem = psutil.virtual_memory()
        total_memory_gb = round(mem.total / (1024**3), 2)
        used_memory_gb = round(mem.used / (1024**3), 2)
        free_memory_gb = round(mem.available / (1024**3), 2)

        logger.info(f" Memory Stats - Total: {total_memory_gb} GB, Used: {used_memory_gb} GB, Free: {free_memory_gb} GB")

        return {
            "memory_total_gb": total_memory_gb,
            "memory_used_gb": used_memory_gb,
            "memory_free_gb": free_memory_gb,
        }
    except Exception as e:
        logger.error(f" Error fetching memory stats: {e}")
        return {"memory_total_gb": 0, "memory_used_gb": 0, "memory_free_gb": 0}


def get_swap_memory():
    """Fetches swap memory usage."""
    try:
        swap = psutil.swap_memory()
        return {
            "swap_total_gb": round(swap.total / (1024**3), 2),
            "swap_used_gb": round(swap.used / (1024**3), 2),
            "swap_free_gb": round(swap.free / (1024**3), 2)
        }
    except Exception as e:
        logger.error(f" Error fetching swap memory stats: {e}")
        return {"swap_total_gb": 0, "swap_used_gb": 0, "swap_free_gb": 0}

def get_cpu_stats():
    """Returns CPU context switches and interrupts."""
    try:
        stats = psutil.cpu_stats()
        return {
            "context_switches": stats.ctx_switches,
            "interrupts": stats.interrupts
        }
    except Exception as e:
        logger.error(f" Error fetching CPU stats: {e}")
        return {"context_switches": 0, "interrupts": 0}


def get_disk_usage():
    """Fetches disk usage statistics."""
    try:
        disk = psutil.disk_usage('/')
        total_disk = round(disk.total / (1024**3), 2)  
        used_disk = round(disk.used / (1024**3), 2)
        free_disk = round(disk.free / (1024**3), 2)

        logger.info(f"Disk Space - Total: {total_disk} GB, Used: {used_disk} GB, Free: {free_disk} GB")
        return total_disk, used_disk, free_disk
    except Exception as e:
        logger.error(f" Error fetching disk space: {e}")
        return 0, 0, 0

def get_process_count():
    """Returns the total number of running processes."""
    return len(psutil.pids()) 

def get_network_speed():
    """Runs Speedtest and handles errors gracefully."""
    try:
        st = speedtest.Speedtest()
        st.get_best_server()
        upload_speed = round(st.upload() / 1_000_000, 2)  
        download_speed = round(st.download() / 1_000_000, 2)

        logger.info(f"Local Network - Upload: {upload_speed} Mbps, Download: {download_speed} Mbps")

        return {
            "measurement": "local_network",
            "source": "local",
            "upload_speed": upload_speed,
            "download_speed": download_speed,
            "timestamp": int(time.time_ns()),
        }
    except Exception as e:
        logger.error(f" Speedtest failed: {e}")
        return None 

def send_network_metrics(producer):
    """Handles sending network metrics asynchronously to avoid blocking."""
    while True:
        try:
            network_metrics = get_network_speed()
            if network_metrics:
                producer.send(TOPIC, value=network_metrics)
                producer.flush()
                logger.info(f" [SENT] Local Network Speed Metrics: {network_metrics}")
        except Exception as e:
            logger.error(f" Error in network speedtest: {e}")
        time.sleep(10) 

def get_iperf_metrics():
    """Runs iperf3 test and extracts network throughput using Docker IP."""
    try:
        local_ip = get_local_ip()
        logger.info(f" Using IP {local_ip} for iperf3 test")

        iperf_server_check = subprocess.run(["pgrep", "iperf3"], capture_output=True, text=True)
        if not iperf_server_check.stdout.strip():
            logger.warning(" iperf3 server is NOT running! Restarting...")
            subprocess.Popen(["iperf3", "-s", "-B", "0.0.0.0"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            time.sleep(2)

        result = subprocess.run(["iperf3", "-c", local_ip, "-J", "-R"], capture_output=True, text=True)
        iperf_data = json.loads(result.stdout)

        sum_sent = iperf_data.get("end", {}).get("sum_sent", {}).get("bits_per_second", None)
        sum_received = iperf_data.get("end", {}).get("sum_received", {}).get("bits_per_second", None)

        if sum_sent is None or sum_received is None:
            logger.warning("iperf3 test did not return expected throughput values.")
            return None

        upload_speed = round(sum_sent / 1_000_000, 2)  
        download_speed = round(sum_received / 1_000_000, 2)

        logger.info(f" Docker Network - Upload: {upload_speed} Mbps, Download: {download_speed} Mbps")

        return {
            "measurement": "network_metrics",
            "source": "docker",
            "upload_speed": upload_speed,
            "download_speed": download_speed,
            "timestamp": int(time.time_ns()),
        }
    except Exception as e:
        logger.error(f" Error running iperf3: {e}")
        return None

def get_system_metrics():
    """Fetches CPU, Memory, Disk, and Process count."""
    total_disk, used_disk, free_disk = get_disk_usage()
    memory = get_memory_usage() 
    
    return {
        "measurement": "system_metrics",
        "cpu_usage": get_cpu_usage(),
        "docker_memory_total_gb": memory["memory_total_gb"],
        "docker_memory_used_gb": memory["memory_used_gb"],
        "docker_memory_free_gb": memory["memory_free_gb"],
        "disk_total": total_disk,
        "disk_used": used_disk,
        "disk_free": free_disk,
        "process_count": get_process_count(),
        "timestamp": int(time.time_ns()),
    }

def get_load_average():
    """Returns system load average over 1, 5, and 15 minutes."""
    return os.getloadavg()

def get_network_usage():
    """Fetches network usage (bytes sent/received per second)."""
    net1 = psutil.net_io_counters()
    time.sleep(1)
    net2 = psutil.net_io_counters()
    return {
        "bytes_sent": (net2.bytes_sent - net1.bytes_sent) / 1024,
        "bytes_received": (net2.bytes_recv - net1.bytes_recv) / 1024
    }

def get_cpu_frequency():
    """Returns CPU frequency in MHz."""
    return psutil.cpu_freq().current

def get_battery_status():
    """Safely returns battery percentage and power status, or None if unavailable."""
    try:
        if hasattr(psutil, "sensors_battery"):
            battery = psutil.sensors_battery()
            if battery:
                return {
                    "percent": battery.percent,
                    "plugged_in": battery.power_plugged
                }
        return None  
    except FileNotFoundError:
        return None  
    except Exception as e:
        logger.warning(f" Could not get battery status: {e}")
        return None

def get_system_uptime():
    """Returns system uptime in seconds."""
    return round(time.time() - psutil.boot_time())

def get_io_usage():
    """Fetches disk read/write speed in KB per second."""
    io1 = psutil.disk_io_counters()
    time.sleep(1)
    io2 = psutil.disk_io_counters()
    return {
        "read_speed": (io2.read_bytes - io1.read_bytes) / 1024,
        "write_speed": (io2.write_bytes - io1.write_bytes) / 1024
    }

def get_tcp_connections():
    """Returns the number of active TCP connections."""
    return len(psutil.net_connections(kind='tcp'))

def get_process_with_highest_cpu():
    """Returns the process consuming the most CPU."""
    processes = [(p.pid, p.info) for p in psutil.process_iter(attrs=['cpu_percent', 'name'])]
    top_process = max(processes, key=lambda p: p[1]['cpu_percent'], default=None)
    return top_process[1] if top_process else None

def get_process_with_highest_memory():
    """Returns the process consuming the most memory."""
    processes = [(p.pid, p.info) for p in psutil.process_iter(attrs=['memory_percent', 'name'])]
    top_process = max(processes, key=lambda p: p[1]['memory_percent'], default=None)
    return top_process[1] if top_process else None

def get_additional_metrics():
    """Fetches additional system metrics."""
    metrics = {
        "measurement": "additional_metrics",
        "load_average": get_load_average(),
        "network_usage": get_network_usage(),
        "cpu_frequency": get_cpu_frequency() or 0,
        "battery_status": get_battery_status() or {"percent": 0, "plugged_in": False},
        "uptime": get_system_uptime(),
        "io_usage": get_io_usage(),
        "tcp_connections": get_tcp_connections() or 0,
        "top_cpu_process": get_process_with_highest_cpu() or {},
        "top_memory_process": get_process_with_highest_memory() or {},
        "cpu_stats": get_cpu_stats(),
        "swap_memory": get_swap_memory(),
        "timestamp": int(time.time_ns()),
    }

    logger.info(f" [DEBUG] Additional Metrics Generated: {json.dumps(metrics, indent=2)}")
    return metrics


def main():
    producer = wait_for_kafka()
    threading.Thread(target=send_network_metrics, args=(producer,), daemon=True).start()

    latency_log = {
        "system": [],
        "iperf3": [],
        "additional": []
    }

    while True:
        try:
          
            start = time.perf_counter()
            system_metrics = get_system_metrics()
            mid = time.perf_counter()
            producer.send(TOPIC, value=system_metrics)
            producer.flush()
            latency = (time.perf_counter() - start) * 1000
            latency_log["system"].append(latency)
            logger.info(f" [SENT] System Metrics (Latency: {latency:.2f} ms): {system_metrics}")

           
            start = time.perf_counter()
            iperf_metrics = get_iperf_metrics()
            if iperf_metrics:
                producer.send(TOPIC, value=iperf_metrics)
                producer.flush()
                latency = (time.perf_counter() - start) * 1000
                latency_log["iperf3"].append(latency)
                logger.info(f" [SENT] Docker Network Metrics (Latency: {latency:.2f} ms): {iperf_metrics}")

           
            start = time.perf_counter()
            additional_metrics = get_additional_metrics()
            if additional_metrics:
                producer.send(TOPIC, value=additional_metrics)
                producer.flush()
                latency = (time.perf_counter() - start) * 1000
                latency_log["additional"].append(latency)
                logger.info(f" [SENT] Additional Metrics (Latency: {latency:.2f} ms): {additional_metrics}")

            
            if len(latency_log["system"]) % 60 == 0:
                logger.info("--- Latency Averages (last 60s) ---")
                for key, values in latency_log.items():
                    if values:
                        avg = mean(values[-60:])
                        logger.info(f"   {key.capitalize()} Avg Latency: {avg:.2f} ms")

        except Exception as e:
            logger.error(f" Error in producer loop: {e}")

        time.sleep(1)


if __name__ == "__main__":
    main()