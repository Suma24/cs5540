from pyspark.sql import SparkSession
import pandas as pd

spark = SparkSession \
    .builder \
    .appName("Twitter Data Analysis") \
    .getOrCreate()

df = spark.read.json("importedtweetsdata.json")
df.createOrReplaceTempView("SmartPhones")

# 10 states with their Total Tweets Count in the United States
sqlDF = spark.sql("SELECT user.location,count(text) as Total_count FROM SmartPhones WHERE place.country='United States' AND user.location is not null GROUP BY user.location ORDER BY Total_count DESC LIMIT 10")

pd = sqlDF.toPandas()
pd.to_csv('output10.csv', index=False)


