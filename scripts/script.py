import sys
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("demo").getOrCreate()

df = spark.read.json("../data/WorldExpenditures.csv")

df.where("age > 21").select("name.first").show()

