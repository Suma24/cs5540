from pyspark.sql import SparkSession
import pandas as pd

spark = SparkSession \
        .builder \
        .appName("Twitter Data Analysis") \
        .getOrCreate()

df = spark.read.json("importedtweetsdata.json")
df.createOrReplaceTempView("SmartPhones")

# Total Tweets Tweeted per Month
sqlDF = spark.sql("SELECT substring(user.created_at,5,3) as Month, count(user.id) as Total_Tweets_per_Month from SmartPhones where user.created_at <> 'null' group by Month")

pd = sqlDF.toPandas()
pd.to_csv('output1.csv', index=False)

