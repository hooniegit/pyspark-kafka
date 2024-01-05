import os, sys
from configparser import ConfigParser

# SET CONFIGs
conf_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "../config/config.ini")
config = ConfigParser()
config.read(conf_dir)

# SET LIBs
lib_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "../lib")
sys.path.append(lib_dir)

from consumer import receive_json
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master(config.get('spark', 'master')) \
    .appName("kafka_stream_demo") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .getOrCreate()

kafka_bootstrap_servers = config.get('kafka', 'bootstrap_servers')
input_kafka_topic = "test"

df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", input_kafka_topic) \
    .load()

query = df \
    .writeStream \
    .foreachBatch(lambda batchDF, batchId: receive_json(batchDF, batchId, spark)) \
    .start()

query.awaitTermination()
