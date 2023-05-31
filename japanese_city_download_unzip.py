
import urllib.request
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

# Stop the Spark session
spark.stop()