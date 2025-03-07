FROM python:3.9

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Install iperf3
RUN apt-get update && apt-get install -y iperf3

# Copy all files
COPY . .

# Run the producer script
CMD ["python", "producer.py"]
