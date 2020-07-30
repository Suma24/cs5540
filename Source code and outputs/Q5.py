from pyspark.sql import SparkSession
import pandas as pd

spark = SparkSession \
    .builder \
    .appName("Twitter Data Analysis") \
    .getOrCreate()

df = spark.read.json("importedtweetsdata.json")
df.createOrReplaceTempView("SmartPhones")

# Users created per Year
sqlDF = spark.sql("SELECT substring(user.created_at,27,4) as Year,count(*) as Total from SmartPhones where user.created_at is not null group by substring(user.created_at,27,4) order by Year desc")

pd = sqlDF.toPandas()
pd.to_csv('output5.csv', index=False)



