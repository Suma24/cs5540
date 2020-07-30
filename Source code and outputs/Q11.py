from pyspark.sql import SparkSession
import pandas as pd

spark = SparkSession \
    .builder \
    .appName("Twitter Data Analysis") \
    .getOrCreate()

df = spark.read.json("importedtweetsdata.json")
df.createOrReplaceTempView("SmartPhones")

# Top10 Languages used by Tweets
sqlDF = spark.sql("select count(*) as Total_count, lang as Language from SmartPhones group by lang order by Total_count desc limit 10")

pd = sqlDF.toPandas()
pd.to_csv('output11.csv', index=False)


