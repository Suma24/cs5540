from pyspark.sql import SparkSession
import pandas as pd

spark = SparkSession \
        .builder \
        .appName("Twitter Data Analysis") \
        .getOrCreate()

df = spark.read.json("importedtweetsdata.json")
df.createOrReplaceTempView("SmartPhones")

# Top5 most Followed Users related to Samsung Tweets
sqlDF = spark.sql("SELECT substring(user.screen_name,0,10) as User,max(user.followers_count) as Followers FROM SmartPhones WHERE text like '%samsung%' group by User order by Followers desc limit 5")

pd = sqlDF.toPandas()
pd.to_csv('output13.csv', index=False)


