from pyspark.sql import SparkSession
import pandas as pd

spark = SparkSession \
    .builder \
    .appName("Twitter Data Analysis") \
    .getOrCreate()

df = spark.read.json("importedtweetsdata.json")
df.createOrReplaceTempView("SmartPhones")

# Top10 Users with the most Retweets
sqlDF = spark.sql("SELECT user.screen_name,retweeted_status.retweet_count FROM SmartPhones ORDER BY retweeted_status.retweet_count DESC LIMIT 10")

pd = sqlDF.toPandas()
pd.to_csv('output12.csv', index=False)


