from pyspark.sql.session import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("Machine Sensors")\
			.config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector:10.0.0")\
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.3")\
            .master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

kafka_df = spark.readStream.format("kafka").option("kafka.bootstrap.servers","localhost:9092").option("subscribe","sensor").load()
kafka_df1 = kafka_df.selectExpr("cast(value as string)")
kafka_df2 = kafka_df1.withColumn("csv",regexp_replace(kafka_df1.value,"\"",""))
kafka_df3 = kafka_df2.withColumn("splitted",split(kafka_df2.csv,","))
kafka_df4 = kafka_df3.withColumn("timestamp",col("splitted").getItem(0).cast('timestamp')) \
					.withColumn("sensor_id", col("splitted").getItem(1)) \
					.withColumn("temperature", col("splitted").getItem(2).cast("Double")) \
					.withColumn("humidity", col("splitted").getItem(3).cast("Double")) \
					.withColumn("water_level", col("splitted").getItem(4).cast("Double")) \
					.withColumn("pressure", col("splitted").getItem(5).cast("Double")) \
                    .withColumn("machine_value", col("splitted").getItem(6).cast("Double")) \
					.drop("value", "csv", "splitted")

kafka_df4.writeStream.format("console").outputMode("update").option("truncate","false").start()
kafka_df4.writeStream.format("mongodb")\
    .queryName("ToMDB")\
    .option("checkpointLocation", "/tmp/pyspark7/")\
    .option('spark.mongodb.connection.uri', 'mongodb://bi_user:biuser1@mongodb.prod.bdb.ai:27017/test?authSource=bi_testing&readPreference=primary&appname=MongoDB%20Compass&directConnection=true&ssl=false')\
    .option('spark.mongodb.database', 'bi_testing')\
    .option('spark.mongodb.collection', 'sensors')\
    .trigger(continuous="10 seconds")\
    .outputMode("append")\
    .start().awaitTermination()

#kafka_df4.writeStream.foreachBatch(lambda df,epoch_id :df.write.format("console").option("truncate","false").save()).start().awaitTermination()