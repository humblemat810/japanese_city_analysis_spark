#%%
from pyspark.sql import SparkSession
#driver_path = "/opt/mysql-connector-j-8.0.33/mysql-connector-j-8.0.33.jar"
#driver_path = "hdfs\://localhost\:9000/user/pchan/library/jar/mysql-connector-j-8.0.33.jar"
driver_path = "/opt/mysql-connector-j-8.0.33/mysql-connector-j-8.0.33.jar"

#format : hdfs://<namenode-hostname>:9000/user/hadoop/jars/driver.jar
# Create a Spark session
spark = SparkSession.builder.appName("jp address download")\
        .config("spark.driver.extraClassPath", driver_path)\
        .config("spark.executor.extraClassPath", driver_path)\
        .getOrCreate()
#%%
from pyspark.conf import SparkConf
current_classpath = SparkConf().get('spark.driver.extraClassPath')
print(current_classpath)
#%%
sc = spark.sparkContext
driver_path = "/opt/mysql-connector-j-8.0.33/mysql-connector-j-8.0.33.jar"

sc.addFile(driver_path)
#%%
#JDBC 
jdbc_driver = "com.mysql.jdbc.Driver"
url = "jdbc:mysql://127.0.0.1:3306/JP_ADDRESS_ANALYSIS"
properties = {"driver": jdbc_driver, "user": "root", 
              "password": "my-secret-pw"}
#%% test load from jdbc
df = spark.read.format("jdbc") \
    .option("url", url) \
    .option("driver", jdbc_driver) \
    .option("dbtable", "JP_ADDRESS") \
    .option("user", "root") \
    .option("password", "my-secret-pw") \
    .load()



# Print the first few rows of the DataFrame
df.limit(100).show(5)

#%%

from pyspark.sql.functions import explode, split, desc, lit, sum, col, max
from pyspark.sql.window import Window
df2 = df.withColumn("machi_jp_chars", split(df.machi_jp, "").cast("char(1)"))

# Explode the array of characters into individual rows using the explode function
df2 = df2.select(explode(df2.machi_jp_chars).alias("machi_jp_char"))
df2 = df2.groupBy("machi_jp_char").count().withColumnRenamed("count", "frequency").orderBy(desc("frequency"))
df2.show()
#%%
w = Window.partitionBy().orderBy(col("frequency").desc() )
df3 = df2.withColumn("cumsum_frequency", sum(col("frequency")).over(w))
url = "jdbc:mysql://127.0.0.1/JP_ADDRESS_ANALYSIS"
df3.write.jdbc(url=url, table="JP_ADDRESS_WORD_FREQUENCIES", 
               mode="overwrite", 
               properties=properties)
df3.limit(100).show()
#%%
total_cnt = df3.agg(sum("frequency")).collect()[0][0]
broadcastVar_total_cnt = sc.broadcast(total_cnt)
broadcastVar_total_cnt.value
df3.agg(max("cumsum_frequency")).show()

# %%
