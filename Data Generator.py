import random
import sys
import time
import pandas as pd
import datetime
import paho.mqtt.client as mqtt
import json

def generator():
    client = mqtt.Client()
    client.connect('localhost',1883,60)
    while True:
        temperature = round(random.uniform(-25.0,45.0),2)
        humidity = round(random.uniform(0,65.0),2)
        waterLevel = round(random.uniform(25.0, 45.0), 2)
        pressure = round(random.uniform(150,300), 2)
        value =  round(random.uniform(10,20), 2)
        dt = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        if temperature is not None and humidity is not None:
            body = "%s,P1M1_S1,%s,%s,%s,%s,%s" % (dt,temperature,humidity,waterLevel,pressure,value)
            print(str(body))
            client.publish('sensor',str(body))
            time.sleep(3)
        else:
            print("Failed to get reading...!")
            sys.exit(1)
    #client.disconnect()

generator()