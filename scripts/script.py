import sys
import pandas as pd
from pyspark.sql import SparkSession


spark = SparkSession.builder.appName("demo").getOrCreate()


file_location = "./data/WorldExpenditures.csv"


panda_df = pd.read_csv(file_location, usecols= ['Year', 'Country' ,'Sector', 'Expenditure(million USD)', 'GDP(%)'])
print (panda_df)


df = spark.createDataFrame(panda_df)
print(df)


