Real-Time System Monitoring Platform
Overview
This project is a full-stack real-time monitoring system that visualizes system performance metrics such as CPU, memory, disk I/O, and network bandwidth. The system uses a Python-based Kafka producer to gather metrics from the host machine and Docker containers, streams them to Kafka, where a Kafka consumer processes and writes them to InfluxDB. These are then visualized via Grafana dashboards with real-time updates.

The full pipeline is containerized using Docker Compose, and all services operate on a shared virtual network.

The platform captures data from both the host and containerized environments using tools like:

psutil (CPU, memory, uptime, TCP)

iperf3 (Docker-to-Docker network)

speedtest-cli (external internet speed)

Dashboards update every 5 seconds and include threshold highlights, time filters, and user-friendly visual breakdowns across 17 real-time panels.

Requirements:
Docker

Docker Compose

Open ports: 3000, 8087, 29092

Step-by-Step Setup
Prerequisites
Ensure Docker and Docker Compose are installed and Docker is running on your machine.

How to Run the System
Clone the repository

Navigate to the project directory

Run the full system using Docker Compose:
docker-compose up --build
Once running, access Grafana at:
http://localhost:3000

Login to Grafana:
Username: admin  
Password: admin
What’s Running Behind the Scenes
Kafka Producer: Collects metrics every second using Python and streams them to Kafka.

Kafka Broker & Zookeeper: Manages topic distribution and message streaming.

Kafka Consumer: Parses and inserts structured metrics into InfluxDB.

InfluxDB: Time-series database storing the collected metrics in a metrics bucket.

Grafana: Displays metrics in dashboards using panels configured for each category.

Dashboard Panels Include
CPU usage, load average, frequency, and top process stats

RAM usage and swap breakdown

Disk I/O speed and usage

System uptime

Network bandwidth (internal & external)

TCP connections and socket growth

make sure Dashboards are configured to auto-refresh every 5 seconds and that they show only the last 10 minutes of data collected unless you change the queries to lengthen or shorten this time/

Services & Credentials
InfluxDB
URL: http://localhost:8087

Username: admin

Password: securepassword

Bucket: metrics

Org: org

Grafana
URL: http://localhost:3000

Username: admin

Password: admin

Kafka
Broker: kafka:9092

Topic: metrics

Features & Testing
Container-to-dashboard metric flow validated under active system load

End-to-end delay: < 1 second

Live panel updates for 17 metrics using real data

Metrics validated with host tools like htop, free -h, df -h, speedtest, ss

Notes & Limitations
Some memory and disk stats reflect the container scope due to Docker isolation

iperf3 may occasionally crash but auto-restarts inside the container

speedtest-cli tests introduce natural latency due to external server dependencies

InfluxDB does not backfill data after outages — missing values are expected

Multi-node scaling is possible by extending Docker services with secure Kafka configs

Dashboard Highlights
CPU spikes during iperf3 load testing

Disk I/O bursts during InfluxDB batch writes

Network throughput variations across Docker and external tests

Real-time socket and process tracking from container and host views


most Common Issue: Services Disconnecting from the Network
A common issue during runtime is that one or more services (like influxdb, kafka_consumer, kafka_producer, zookeeper, or kafka) may disconnect from the Docker network. 
If this happens, Grafana dashboards may appear empty or fail to update in real time.

Run the following command in your terminal to inspect the Docker network: docker inspect finalyearproject_finalyearproject_network
Look under the "Containers" section to confirm that the following services are connected:

zookeeper

kafka

kafka_producer

kafka_consumer

influxdb

If a service is missing from the list, reconnect it manually. For example, to reconnect InfluxDB: docker network connect finalyearproject_finalyearproject_network influxdb
