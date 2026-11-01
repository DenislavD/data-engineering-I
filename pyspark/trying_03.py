# Transformations and actions in RDDs

from pyspark.sql import SparkSession

data = [
    (1, 'Elena',   28, 'HR'),
    (2, 'Ivan',    45, 'IT'),
    (3, 'Yasen',   33, 'Finance'),
    (4, 'Olq',     18, 'Marketing'),
    (5, 'Bali',    39, 'IT')
]

spark = SparkSession.builder.appName('test1').getOrCreate()
columns = 'employee_id', 'name', 'age', 'department'
df = spark.createDataFrame(data, columns)


sc = spark.sparkContext

rdd = sc.parallelize( range(1, 11) )
filtered_rdd = rdd.filter(lambda x: x % 2 == 0)
print(filtered_rdd.collect())
sum_result = rdd.reduce(lambda x, y: x + y) # reduces to 1 element
print('Sum of result:', sum_result)

rdd_grouped = rdd.groupBy(lambda x: x % 2)
result = [(k, list(v)) for k,v in rdd_grouped.collect()]
print(result)

rdd2 = sc.parallelize(['hello world', 'Hello, Spark'])
rdd2_flat = rdd2.flatMap(lambda x: x.split())
rdd2_flat.collect() # ['hello', 'world', 'Hello,', 'Spark']

rdd3 = sc.parallelize( [('apple', 1), ('banana', 2), ('apple', 3), ('banana', 4)] )
rdd3_reduced = rdd3.reduceByKey(lambda x, y: x + y)
rdd3_reduced.collect() # [('apple', 4), ('banana', 6)]

rdd_union = rdd2.union(rdd3)
rdd_union = rdd2.union(rdd3)
rdd_union.collect()
o = rdd_union.take(3)
print(o)

import os
os.chdir(os.path.dirname(__file__)) # doesn't work, still gets the original starting dir??
# rdd_union.saveAsTextFile('outputs_1') # creates a directory with partitioned files
