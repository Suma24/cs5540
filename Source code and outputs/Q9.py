from pyspark.sql import SparkSession
import pandas as pd

spark = SparkSession \
    .builder \
    .appName("Twitter Data Analysis") \
    .getOrCreate()

df = spark.read.json("importedtweetsdata.json")
df.createOrReplaceTempView("SmartPhones")

# Top10 Verified User Accounts with most Followers
sqlDF = spark.sql("SELECT user.verified,user.screen_name,max(user.followers_count) as followers_count FROM SmartPhones WHERE user.verified = true GROUP BY user.verified, user.screen_name LIMIT 10")

pd = sqlDF.toPandas()
pd.to_csv('output9.csv', index=False)


