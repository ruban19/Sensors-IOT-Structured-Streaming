# Sensors-IOT-Structured-Streaming

Hi there,

This is a Real Case POC of Machine Sensors Structured Streaming 

Sensors ==> MQTT ==> KAFKA ==> Spark ==> MongoDB

Data Generator.py is the python file which generates the data for the sensors and sends to MQTT topic

Mqtt to kafka.py is the python file which consumes data from MQTT topic and publish it to Kafka Topic

Spark Streaming.py is the python file which subscribe data from Kafka topic and convert the data to DataFrame and writes to MongoDB

![Screenshot from 2022-08-07 20-12-24](https://user-images.githubusercontent.com/91327631/183296545-1daf8f0f-4e69-4cb3-8711-df621a9f85d9.png)
