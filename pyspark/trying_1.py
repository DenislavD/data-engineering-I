# just a test, it probably isn't intended to be run like this - spinning the
# Spark environment, WebUI port only to create 2 small DFs and exit everything

import pyspark # installed globally on local
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('my4').getOrCreate()

print('Spark version:', spark.version)

data = [('Alice', 34), ('Bob', 45), ('Charlie', 23)]
df1 = spark.createDataFrame(data, ['Name', 'Age'])
df1.show()

csv_path = r'D:/Programming/python/Learning/basics/locations.csv'
df2 = spark.read.csv(csv_path, header=False, inferSchema=True)
df2.show()

