FROM python:3.9
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
RUN pip install numpy
RUN apt-get update && apt-get install -y iperf3
COPY . .
CMD ["python", "producer.py"]
