from pyspark.sql import SparkSession
import pandas as pd

spark = SparkSession \
    .builder \
    .appName("Twitter Data Analysis") \
    .getOrCreate()

df = spark.read.json("importedtweetsdata.json")
df.createOrReplaceTempView("SmartPhones")

# Tweet Count from Top10 Countries
sqlDF = spark.sql("SELECT place.country,count(*) AS count FROM SmartPhones where place.country <> 'null' GROUP BY place.country ORDER BY count DESC limit 10")

pd = sqlDF.toPandas()
pd.to_csv('output6.csv', index=False)


