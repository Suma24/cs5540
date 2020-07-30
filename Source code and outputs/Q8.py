from pyspark.sql import SparkSession
import pandas as pd

spark = SparkSession \
    .builder \
    .appName("Twitter Data Analysis") \
    .getOrCreate()

df = spark.read.json("importedtweetsdata.json")
df.createOrReplaceTempView("SmartPhones")

# Top5 most Followed Users related to Moto Tweets
sqlDF = spark.sql("SELECT user.name, max(user.followers_count) as Followers FROM SmartPhones WHERE text like '%moto%' group by user.name order by Followers desc limit 5")

pd = sqlDF.toPandas()
pd.to_csv('output8.csv', index=False)


