from kafka import KafkaAdminClient, KafkaProducer
from kafka.admin import NewTopic
from kafka.errors import UnknownTopicOrPartitionError
import weather 
from report_pb2 import Report
import time
broker = 'localhost:9092'
admin_client = KafkaAdminClient(bootstrap_servers=[broker])

try:
    admin_client.delete_topics(["temperatures"])
    print("Deleted topics successfully")
except UnknownTopicOrPartitionError:
    print("Cannot delete topic/s (may not exist yet)")

time.sleep(3) # Deletion sometimes takes a while to reflect

# TODO: Create topic 'temperatures' with 4 partitions and replication factor = 1

admin_client.create_topics([NewTopic("temperatures", num_partitions=4, replication_factor=1)])

#print("Topics:", admin_client.list_topics())
producer = KafkaProducer(bootstrap_servers=[broker], retries = 10, acks = "all")
months = {
    "01": "January",
    "02": "February",
    "03": "March",
    "04": "April",
    "05": "May",
    "06": "June",
    "07": "July",
    "08": "August",
    "09": "September",
    "10": "October",
    "11": "November",
    "12": "December"
}

# Runs infinitely because the weather never ends
for date, degrees in weather.get_next_weather(delay_sec=0.1):
    print(date)
    if degrees > 1000:
        print("error" + date)
    #print(date, degrees) # date, max_temperature
    month = date[5:7]
    key = months[month]
    message = Report(date = date, degrees = degrees)
    msgSerialized = message.SerializeToString()
    producer.send("temperatures", msgSerialized, bytes(key,"utf-8"))

