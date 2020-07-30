from pyspark.sql import SparkSession
import pandas as pd

spark = SparkSession \
        .builder \
        .appName("Twitter Data Analysis") \
        .getOrCreate()

df = spark.read.json("importedtweetsdata.json")
df.createOrReplaceTempView("SmartPhones")

# Tweets based on Phone Companies
sqlDF = spark.sql("select count(*) as Tweets, 'Iphone' as Company from SmartPhones where text like '%ios%' or text like '%iphone%' or text like '%apple%' UNION select count(*) as Tweets, 'Samsung' as Company from SmartPhones where text like '%samsung%' UNION select count(*) as Tweets, 'Moto' as Company from SmartPhones where text like '%moto%' or text like '%motorola%' UNION select count(*) as Tweets, 'Redmi' as Company from SmartPhones where text like '%redmi%' UNION select count(*) as Tweets, 'Xiaomi' as Company from SmartPhones where text like '%xiaomi%' UNION select count(*) as Tweets, 'Nokia' as Company from SmartPhones where text like '%nokia%' UNION select count(*) as Tweets, 'Lenovo' as Company from SmartPhones where text like '%lenovo%' UNION select count(*) as Tweets, 'Oppo' as Company from SmartPhones where text like '%oppo%' UNION select count(*) as Tweets, 'OnePlus' as Company from SmartPhones where text like '%oneplus%'  UNION select count(*) as Tweets, 'BlackBerry' as Company from SmartPhones where text like '%blackberry%' UNION select count(*) as Tweets, 'HTC' as Company from SmartPhones where text like '%htc%'")

pd = sqlDF.toPandas()
pd.to_csv('output2.csv', index=False)


