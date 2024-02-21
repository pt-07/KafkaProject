from kafka import KafkaConsumer, TopicPartition
import json
import os
import sys
import report_pb2
from datetime import datetime

# Set the output directory
output_dir = '/files'  # Adjust this to your desired directory path in the Docker container

# Kafka setup
broker = 'localhost:9092'
consumer = KafkaConsumer(bootstrap_servers=[broker], auto_offset_reset='earliest')
partitions = [int(p) for p in sys.argv[1:]]
topic_partitions = [TopicPartition('temperatures', p) for p in partitions]
consumer.assign(topic_partitions)

# Initialize or load partition data
partition_data = {}
for tp in topic_partitions:
    file_path = os.path.join(output_dir, f'partition-{tp.partition}.json')
    if not os.path.exists(file_path):
        partition_data[tp.partition] = {"partition": tp.partition, "offset": 0}
    else:
        with open(file_path, 'r') as file:
            partition_data[tp.partition] = json.load(file)

# Set the initial position for each partition
for tp in topic_partitions:
    consumer.seek(tp, partition_data[tp.partition]["offset"])

# Main loop to consume and process messages
while True:



    batch = consumer.poll(1000)

    for tp, messages in batch.items():
        #print(f"Polling messages from partition {tp.partition}")
        #print(f"Polled {len(messages)} messages from partition {tp.partition}")

        for message in messages:
            report = report_pb2.Report()
            report.ParseFromString(message.value)
            date_str = report.date
            temperature = report.degrees
            print(date_str)

            date = datetime.strptime(date_str, '%Y-%m-%d')
            month = date.strftime('%B')
            year = date.year

            month_data = partition_data[tp.partition].setdefault(month, {}).setdefault(str(year), {"count": 0, "sum": 0, "avg": 0, "start": "", "end": ""})
            #print(month_data)
            if temperature > 1000 and (month_data["end"] != "" and month_data["end"] >= date_str):
                #print("duplicate")
                pass
            elif temperature > 1000 and not (month_data["end"] != "" and month_data["end"] >= date_str):
                print("problem", month_data["end"], date_str)


            if month_data["end"] != "" and month_data["end"] >= date_str:
                # print("skips")
                continue
                
            month_data["end"] = date_str

            month_data["count"] += 1
            month_data["sum"] += temperature
            month_data["avg"] = month_data["sum"] / month_data["count"]

        if messages:
            #partition_data[tp.partition]["offset"] = messages[-1].offset + 1
            partition_data[tp.partition]["offset"] = consumer.position(TopicPartition("temperatures", tp.partition))
            file_path = os.path.join(output_dir, f'partition-{tp.partition}.json')

            # Atomic write operation
            temp_file_path = file_path + '.tmp'
            with open(temp_file_path, 'w') as file:
                json.dump(partition_data[tp.partition], file)
            os.rename(temp_file_path, file_path)
