# just a test, it probably isn't intended to be run like this - spinning the
# Spark environment, WebUI port only to create 2 small DFs and exit everything

# cd "D:\Programming\python\Learning\pyspark" + py -i trying_1.py

import pyspark # installed globally on local
from pyspark.sql import SparkSession, Row

spark = SparkSession.builder.appName('my4').getOrCreate()
# access context with spark.sparkContext

print('Spark version:', spark.version)

data = [
    ('Alice',   28, 'HR'),
    ('Bob',     45, 'IT'),
    ('Charlie', 33, 'Finance'),
    ('Kori',    18, 'Marketing')
]
df1 = spark.createDataFrame(data, ['Name', 'Age'])
df1.show()

csv_path = r'D:/Programming/python/Learning/basics/locations.csv'
df2 = spark.read.csv(csv_path, header=False, inferSchema=True)
df2.show()

df1.printSchema() # shows columns
print(df1.columns, df1.head(1), df1.tail(1))


# caching and broadcasting intro
df1.cache() # saves in memory?

from pyspark.sql import functions as F
broadcasted_df = F.broadcast(df1) # distributed to all worker nodes, avoids shuffling


# create a Resilient Distributed Dataset
# RDDs are immutable, each action creates a new RDD
sc = spark.sparkContext
rdd = sc.parallelize([1,2,3,4,5]) # <class 'pyspark.core.rdd.RDD'>

print('RDD:', rdd.count())

transformed_rdd = rdd.map(lambda x: x * 2)
print('transformed_rdd:', rdd.collect()) # the .collect() action actually starts the job

df3 = transformed_rdd.map(lambda x: Row(single=x//2, doubled=x)).toDF()
df3.show()
