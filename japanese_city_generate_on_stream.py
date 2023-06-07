#%%
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SparkSession
import json
import requests

# state_code = ""
# country_code = ""
# limit = 1
# with open("./config.json", "r") as f:
#     config = json.load(f)
# city_list = config['cities']
# city_lat_lon = {}
# # init spark context
driver_path = "hdfs://localhost:9000/pchan/library/jar/mysql-connector-j-8.0.33.jar"
spark = SparkSession.builder.appName("streamingResponse")\
    .config("spark.driver.extraClassPath", driver_path)\
    .config("spark.executor.extraClassPath", driver_path)\
    .getOrCreate()
sc = spark.sparkContext
#sc = SparkContext("local[*]", "streamingResponse")
ssc = StreamingContext(sc, 2)

lines = ssc.socketTextStream("localhost", 9999)

import mariadb

#%%
def generate(length, *arg, **kwarg):
    return str(length)
def gen_name(x):
    len = get_generated_length()
    name = generate(length = len)
    return x, name, len
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

    for row in rows:
        print(row)
    return row[0]
temp = lines.map(gen_name)
temp.pprint()
lines.pprint()
# df_port_in_stream = spark.readStream
#       .format("socket")
#       .option("host","localhost")
#       .option("port","9090")
#       .load()
# location_cache_fname = 'location_cache.json'
# try:
#     with open(location_cache_fname, 'r') as f:
#         city_lat_lon = json.load(f)
        
# except FileNotFoundError:
#     city_lat_lon = {}
    
# # resolve location name to longitutide and latitude
# cache_need_update = False
# for city_name in city_list:
#     if city_name not in city_lat_lon:
#         location_response = requests.get(f"http://api.openweathermap.org/geo/1.0/direct?q={city_name},{state_code},{country_code}&limit={limit}&appid={config['api_key']}")
#         if len(location_response.json()) == []:
#             raise ValueError("invalid request, city name may not be valid")
#         city_record = location_response.json()[0]
#         lat, lon = city_record['lat'], city_record['lon']
#         city_lat_lon[city_name] = lat, lon
#         cache_need_update = True
# if cache_need_update:
#     with open(location_cache_fname, 'w') as f:
#         json.dump(city_lat_lon,f)

# def get_city_url(city_name):
#     lat, lon = city_lat_lon[city_name]
#     request_url = f"https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={config['api_key']}"
#     return request_url

# def fetch_data(url: str):
#     data = requests.get(url).json()
#     print(data)
#     return sc.parallelize(data)
# from functools import partial
# """
# def fetch_data_factory(url):
#     def wrap():
#         fetch_data(url)
#     return wrap"""
# ff = [partial(fetch_data, url) for url in map(get_city_url, city_lat_lon)]

# lines = ssc.socketTextStream("localhost", 9999)
# dstreams = [ssc.foreachRDD(lambda rdd: f()) for f in ff]

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
