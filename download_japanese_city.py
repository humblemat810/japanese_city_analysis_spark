#%%
import urllib.request
import os

import os

from pyspark.sql import SparkSession

driver_path = "C:\Program Files (x86)\MySQL\Connector J 8.0\mysql-connector-j-8.0.33.jar"
# Create a Spark session
spark = SparkSession.builder.appName("jp address download")\
    .config("spark.driver.extraClassPath", driver_path).getOrCreate()
url = 'https://www.post.japanpost.jp/zipcode/dl/roman/ken_all_rome.zip'
filename = 'ken_all_rome.zip'
data_folder = "./data/"
if not os.path.exists(data_folder):
    os.makedirs(data_folder)
full_download_path = os.path.join(data_folder, filename)
if os.path.exists(full_download_path):
    pass
else:
    urllib.request.urlretrieve(url, full_download_path)


import zipfile
import os

zip_filename = full_download_path
extract_path = data_folder

with zipfile.ZipFile(zip_filename, 'r') as zip_file:
    for file in zip_file.namelist():
        extracted_path = os.path.join(extract_path, file)
        if os.path.exists(extracted_path):
            with open(extracted_path, 'rb') as extracted_file:
                existing_contents = extracted_file.read()
            with zip_file.open(file, 'r') as new_file:
                new_contents = new_file.read()
            if existing_contents != new_contents:
                with open(extracted_path, 'wb') as outfile:
                    outfile.write(new_contents)
        else:
            zip_file.extract(file, extract_path)
#%%
# import pandas as pd

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

from pyspark.sql.functions import explode, split, desc, lit
df2 = df.withColumn("machi_jp_chars", split(df.machi_jp, ""))

# Explode the array of characters into individual rows using the explode function
df2 = df2.select(explode(df2.machi_jp_chars).alias("machi_jp_char"))
df2 = df2.groupBy("machi_jp_char").count().withColumnRenamed("count", "frequency").orderBy(desc("frequency"))
df2.limit(100).show()

from pyspark.sql.functions import current_date
df3 = df2.withColumn('date',current_date())

#%%
# run this after the command 
# """
# docker run --detach --network some-network --name some-mariadb --env MARIADB_USER=example-user --env MARIADB_PASSWORD=my_cool_secret --env MARIADB_ROOT_PASSWORD=my-secret-pw  mariadb:latest
# 
# """

import mariadb
conn = mariadb.connect(
        user="root",
        password="my-secret-pw",
        host="192.168.0.101",
        port=3306,
        database="mysql"
    )
cur = conn.cursor()
cur.execute("show databases;")
rows = cur.fetchall()

for row in rows:
    print(row[0])
def exe_load_show(cur, sql):
    cur.execute("show databases;")
    rows = cur.fetchall()
    print(rows)
    return rows


cur.execute("CREATE DATABASE IF NOT EXISTS JP_ADDRESS_ANALYSIS;")
exe_load_show(cur, "show databases;")
#JDBC 
jdbc_driver = "com.mysql.jdbc.Driver"
url = "jdbc:mysql://192.168.0.101/JP_ADDRESS_ANALYSIS"
properties = {"driver": jdbc_driver, "user": "root", 
              "password": "my-secret-pw"}

# write the DataFrame to the MySQL database
df3.write.jdbc(url=url, table="mytable", 
               mode="overwrite", 
               properties=properties)
#%% test load from jdbc
df_temp = spark.read.format("jdbc") \
    .option("url", url) \
    .option("driver", jdbc_driver) \
    .option("dbtable", "mytable") \
    .option("user", "root") \
    .option("password", "my-secret-pw") \
    .load()



#%%
# Stop the Spark session
spark.stop()
# %%
