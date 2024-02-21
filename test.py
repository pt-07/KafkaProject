import weather
from report_pb2 import Report
from kafka import KafkaAdminClient, KafkaProducer, KafkaConsumer
broker = "localhost:9092"
admin = KafkaAdminClient(bootstrap_servers=[broker])
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
    print(date, degrees) # date, max_temperature
    month = date[5:7]
    key = months[month]
    message = Report(date = date, degrees = degrees)
    msgSerialized = message.SerializeToString()
    producer.send("temperatures", msgSerialized, bytes(key,"utf-8"))
    
