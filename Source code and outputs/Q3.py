from pyspark.sql import SparkSession
import pandas as pd

spark = SparkSession \
        .builder \
        .appName("Twitter Data Analysis") \
        .getOrCreate()

df = spark.read.json("importedtweetsdata.json")
df.createOrReplaceTempView("SmartPhones")

# Top10 User with most Tweets
sqlDF = spark.sql("SELECT count(*) as Count, user.name from SmartPhones where user.name is not null group by user.name order by Count desc limit 10")

pd = sqlDF.toPandas()
pd.to_csv('output3.csv', index=False)


