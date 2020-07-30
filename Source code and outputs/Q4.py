from pyspark.sql import SparkSession
import pandas as pd

spark = SparkSession \
        .builder \
        .appName("Twitter Data Analysis") \
        .getOrCreate()

df = spark.read.json("importedtweetsdata.json")
df.createOrReplaceTempView("SmartPhones")

# Top5 most Followed Users related to Iphone Tweets
sqlDF = spark.sql("SELECT substring(user.screen_name,0,10) as Users,max(user.followers_count) as Followers FROM SmartPhones WHERE text like '%iphone%' or text like '%apple%' or text like '%ios%' group by Users order by Followers desc limit 5")

pd = sqlDF.toPandas()
pd.to_csv('output4.csv', index=False)


