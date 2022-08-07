# Sensors-IOT-Structured-Streaming

Hi there,

This is a Real Case POC of Machine Sensors Structured Streaming 

Sensors ==> MQTT ==> KAFKA ==> Spark ==> MongoDB

Data Generator.py is the python file which generates the data for the sensors and sends to MQTT topic

Mqtt to kafka.py is the python file which consumes data from MQTT topic and publish it to Kafka Topic

Spark Streaming.py is the python file which subscribe data from Kafka topic and convert the data to DataFrame and writes to MongoDB

