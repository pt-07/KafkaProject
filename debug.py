from kafka import KafkaConsumer
from report_pb2 import Report
broker = "localhost:9092"
consumer = KafkaConsumer(bootstrap_servers=[broker])

consumer.subscribe(["temperatures"])

while True:
    batch = consumer.poll(1000)
    for topic_partition, messages in batch.items():
        for msg in messages:
            val = Report.FromString(msg.value)
            # Extracting the required information
            partition = topic_partition.partition
            key = msg.key.decode('utf-8')  # Decoding the key
            date = val.date
            degrees = val.degrees
            # Printing in the desired dictionary format
            print({'partition': partition, 'key': key, 'date': date, 'degrees': degrees})
