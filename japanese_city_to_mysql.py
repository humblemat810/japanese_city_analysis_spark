
import urllib.request
import os

from pyspark.sql import SparkSession


driver_path = "C:\Program Files (x86)\MySQL\Connector J 8.0\mysql-connector-j-8.0.33.jar"
# Create a Spark session
spark = SparkSession.builder.appName("jp address download")\
    .config("spark.driver.extraClassPath", driver_path).getOrCreate()



schema = {
    'zipcode': 'int64',    
    'prefecture_jp': 'string',
    'citymachi_jp': 'string',
    'machi_jp': 'string',
    'prefecture_en': 'string',
    'citymachi_en': 'string',
    'machi_en': 'string'
}
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# Define the Spark schema
spark_schema = StructType([
    StructField('zipcode', IntegerType()),
    StructField('prefecture_jp', StringType()),
    StructField('citymachi_jp', StringType()),
    StructField('machi_jp', StringType()),
    StructField('prefecture_en', StringType()),
    StructField('citymachi_en', StringType()),
    StructField('machi_en', StringType())
])
# Read the CSV file with the specified column names and schema

# df = pd.read_csv("./data/KEN_ALL_ROME.csv",
#                  encoding="cp932", 
#                  names=schema.keys(), 
#                  dtype=schema)

df = spark.read.csv("./data/KEN_ALL_ROME.csv",
                 encoding="cp932", 
                 schema=spark_schema)

# Print the first few rows of the DataFrame
df.show(5)

import mariadb
conn = mariadb.connect(
        user="root",
        password="my-secret-pw",
        host="192.168.0.101",
        port=3306,
        database="mysql"
    )
jdbc_driver = "com.mysql.cj.jdbc.Driver"
url = "jdbc:mysql://192.168.0.101/JP_ADDRESS_ANALYSIS"
properties = {"driver": jdbc_driver, "user": "root", 
              "password": "my-secret-pw"}
from pyspark.sql.functions import current_date
df2 = df.withColumn('last_update',current_date())
df2.write.jdbc(url=url, table="JP_ADDRESS", 
               mode="overwrite", 
               properties=properties)
cur = conn.cursor()
# Stop the Spark session
spark.stop()