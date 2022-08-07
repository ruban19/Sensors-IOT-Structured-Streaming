import os
import paho.mqtt.client as mqtt
import time
import pandas as pd
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
import json

topic_mqtt = 'sensor'
topic_kafka = 'sensor'

def send_message(message):
    print(str(message).replace("\"",""))
    producer.send(topic_kafka,message)


def mqtt_to_kafka_run():
    client_name = "home_connector_%s" % os.getenv("localhost")
    client = mqtt.Client(client_id= client_name)

    on_connect = lambda client,userdata,lags,rc : client.subscribe(topic_mqtt)
    client.on_connect = on_connect

    on_message = lambda client,userdata, message: send_message(message.payload.decode())
    client.on_message = on_message

    on_disconnect = lambda client,user_data,rc: print("""disconnected %s %s %s """ % (client,user_data,rc))
    client.on_disconnect = on_disconnect

    client.connect('localhost', 1883, 60)
    client.loop_forever()


if __name__ == '__main__':
    attempts = 0
    while attempts < 10 :
        try:
            producer = KafkaProducer(bootstrap_servers = ['localhost:9092'],value_serializer= lambda x : json.dumps(x).encode('utf-8'))
            mqtt_to_kafka_run()
        except NoBrokersAvailable:
            print("No Brokers.Attempt - %s" % attempts)
            attempts += 1
