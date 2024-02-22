import sys
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("demo").getOrCreate()

df = spark.read.csv("./data/WorldExpenditures.csv")
df.printSchema()




