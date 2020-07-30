from pyspark.sql import SparkSession
import pandas as pd

spark = SparkSession \
    .builder \
    .appName("Twitter Data Analysis") \
    .getOrCreate()

df = spark.read.json("importedtweetsdata.json")
df.createOrReplaceTempView("SmartPhones")

# Total Tweets per Day
sqlDF = spark.sql("SELECT substring(quoted_status.created_at,1,3) as Days,count(text) as Total_Tweets_per_Day FROM SmartPhones where quoted_status.created_at <> 'null' GROUP BY Days")

pd = sqlDF.toPandas()
pd.to_csv('output7.csv', index=False)

