    from pyspark.sql import SparkSession
from pyspark import SparkFiles
from pyspark import SQLContext

spark = SparkSession.builder.getOrCreate()
url = "https://raw.githubusercontent.com/sathishk38/PRA-MIS-Project/main/CSV-FEED/DA_AI_D_20230724_123000.csv"
spark.sparkContext.addFile(url)
sqlContext = SQLContext(sc)
df = sqlContext.read.csv(
    "dbfs:/local_disk0/spark-787fd2ef-f6a9-4695-b852-e05e37458590/userFiles-67967465-1a9c-4655-a2fb-35aae076fd95/DA_AI_D_20230724_123000.csv",
    inferSchema=True,
    header=True,
)

import pandas as pd
import io
import requests
url = "https://github.com/sathishk38/PRA-MIS-Project/blob/main/CSV-FEED/DA_AI_D_20230724_123000.csv"
read_data = requests.get(url).content
address=pd.read_csv(io.StringIO(read_data.decode('utf-8')))
# csv_df = spark.read.format("csv").option("Header","True") \
#     .load(address)
display(address)

%python
url = "https://github.com/sathishk38/PRA-MIS-Project/blob/main/CSV-FEED/DA_AI_D_20230724_123000.csv"
spark.sparkContext.addFile(url)

df = spark.read.csv("file://" + SparkFiles.get("DA_AI_D_20230724_123000.csv"),header=True,inferSchema=True)
display(df)