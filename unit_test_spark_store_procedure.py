# 
"""this code test the stored procedure on mysql and test call from pyspark"""
from pyspark.sql import SparkSession
driver_path = "/opt/mysql-connector-j-8.0.33/mysql-connector-j-8.0.33.jar"
        # Create a Spark session
spark = SparkSession.builder.appName("jp address to mysql")\
        .config("spark.driver.extraClassPath", driver_path).getOrCreate()

sc = spark.sparkContext

spark = SparkSession.builder \
    .appName("CallStoredProcedure") \
    .getOrCreate()


#%%
import mariadb
conn = mariadb.connect(
        user="root",
        password="my-secret-pw",
        host="127.0.0.1",
        port=3306,
        database="JP_ADDRESS_ANALYSIS"
    )


# %%
from collections import Counter
chcnter = Counter()
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
print(Counter(yield_ch(1000)).most_common())
# %%
