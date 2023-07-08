#%%
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SparkSession
import json
import requests

import os
os.environ['HADOOP_CONF_DIR'] = '/opt/hadoop-3.3.5/etc/hadoop'
from pyspark import SparkConf

SCALA_HOME = "/opt/scala-2.12.17/lib"
import os
sc_paths = []
for i in os.listdir(SCALA_HOME):
    sc_paths.append(str(os.path.join(SCALA_HOME, i)))
sc_path_concat = ','.join(sc_paths)
all_jars = sc_paths
all_jar_path_concat = ','.join(all_jars)
#driver_path = "hdfs://localhost:9000/pchan/library/jar/mysql-connector-j-8.0.33.jar"
spark = SparkSession.builder.appName("streamingResponse")\
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0") \
    .config("spark.jars", all_jar_path_concat) \
    .config("fs.defaultFS","file:///")\
    .getOrCreate()

# .config("spark.hadoop.fs.defaultFS","file:///")\
#print(spark.sparkContext._conf.get("spark.hadoop.fs.defaultFS"))

"""
    .config("spark.jars", all_jar_path_concat) \
    .config("spark.driver.extraClassPath", driver_path)\
    .config("spark.executor.extraClassPath", driver_path)\

    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:3.4.0,org.apache.kafka:kafka-clients:3.4.0") \
    .config("spark.jars.packages", "org.apache.kafka:kafka-clients:3.4.0") \
    .config("spark.jars.packages", "org.apache.spark:spark-token-provider-kafka-0-10_2.13:3.4.0") \
    .config("spark.jars.packages", "org.apache.spark:spark-streaming-kafka-0-10-assembly_2.13:3.4.0") \
    """

#,org.apache.commons:commons-pool2:2.8.0
sc = spark.sparkContext

df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "localhost:9092") \
  .option("subscribe", "demo_java") \
  .option("startingOffsets", "earliest") \
  .load()
df2 = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

console_sink = df.writeStream.format("console").format("console")\
            .outputMode("append")

from pyspark.sql.functions import to_timestamp, col
df3=df2.withColumn("timestamp", to_timestamp(col("value"), "dd/MM/yyyy HH:mm:ss"))
df4=df3.where(col("timestamp").isNotNull()).select(["timestamp"])

"""
An error occurred while calling o39.load. : java.lang.NoClassDefFoundError: scala/$less$colon$less at org.apache.spark.sql.kafka010.KafkaSourceProvider.org$apache$spark$sql$kafka010$KafkaSourceProvider$$validateStreamOptions(KafkaSourceProvider.scala:338)
"""


#sc = SparkContext("local[*]", "streamingResponse")
ssc = StreamingContext(sc, 2)

lines = ssc.socketTextStream("localhost", 9999)
import mariadb

#%%
def generate(length, *arg, **kwarg):
    def yield_ch(n = 1000):
        for i in range(n):
            statement = """
            CALL rand_char(@temp);
            """
            cur = conn.cursor()
            cur.execute(statement)
            conn.commit()

            statement = """
            select @temp;
            """
            cur.execute(statement)
            rows = cur.fetchall()

            # for row in rows:
            #     print(row[0])
            yield rows[0][0]
    import mariadb
    conn = mariadb.connect(
            user="root",
            password="my-secret-pw",
            host="127.0.0.1",
            port=3306,
            database="JP_ADDRESS_ANALYSIS"
        )
    result = ''.join(list(yield_ch(length)))
        
    return result
def gen_name_df():
    len = get_generated_length()
    name = generate(length = len)
    return name 
def gen_name_rdd(x):
    len = get_generated_length()
    name = generate(length = len)
    print('single_gen_name_rdd', name)
    return name 

def get_generated_length():
    import mariadb
    conn = mariadb.connect(
            user="root",
            password="my-secret-pw",
            host="127.0.0.1",
            port=3306,
            database="mysql"
        )
    cur = conn.cursor()
    sql_query_random_row = """
    SELECT CHAR_LENGTH(machi_jp), machi_jp FROM JP_ADDRESS_ANALYSIS.JP_ADDRESS
    ORDER BY RAND()
    LIMIT 1
    """
    cur.execute(sql_query_random_row)
    rows = cur.fetchall()

    return rows[0][0]
generated_name_rdd = lines.map(gen_name_rdd)

generated_name_rdd.foreachRDD(lambda x: print('collected a ', x.collect()))


import socket
socket = socket.socket()
socket.connect(("localhost", 9998)) # test with nc -lk 9998

def write_to_socket(rdd):    
   
    records = rdd.collect()
    print("---records------")
    print(records)
    for record in records:
         socket.send((record+'\n').encode())

# Write to socket from the driver    
generated_name_rdd.foreachRDD (write_to_socket)

from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType, StringType

gen_name_udf = udf(gen_name_df, StringType())

df5 = df4.limit(10).withColumn("name_generated", gen_name_udf())
df6 = df5.withColumnRenamed("name_generated", "value")\
         .withColumnRenamed("timestamp", "key")\
         .withColumn("key", col("key").cast(StringType()))\
         .withColumn("value", col("value").cast(StringType()))


console_sink2 = df6.writeStream.format("console")\
            .option("numRows",10000)\
            .outputMode("append")
#console_sink.start()
kafka_sink = df6.writeStream\
        .format("kafka").outputMode("append")\
        .option("checkpointLocation", "./checkpoint/")\
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("topic", "name_suggestion")
kafka_sink.start()
console_sink2.start()


# to test: nc -lk 9999
# # accumulate the data and compute the average every 60 seconds
# accumulated_data = lines.reduce(lambda x, y: x.union(y)).window(60, 10)
# average_data = accumulated_data.foreachRDD(lambda rdd: print("Average data: {}".format(rdd)))

#%%

# start the streaming context
ssc.start()
ssc.awaitTermination()

# stop the SparkContext object
sc.stop()
