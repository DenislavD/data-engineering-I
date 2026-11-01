# RDD - the original abstraction for distributed data in Spark
# offers fault tolerance, paralellism and immutability
# can be used for unstructured data, lower level operations

# DataFrame - higher-level abstraction built on top of RDDs
# schema-based (columns), optimized execution, perfect for queries

# filter, aggregates, joins
from pyspark.sql import SparkSession

data = [
    (1, 'Elena',   28, 'HR'),
    (2, 'Ivan',    45, 'IT'),
    (3, 'Yasen',   33, 'Finance'),
    (4, 'Olq',     18, 'Marketing')
]

spark = SparkSession.builder.appName('OperationsExample').getOrCreate()
columns = 'employee_id', 'name', 'age', 'department'
df = spark.createDataFrame(data, columns)

# filter
df_filtered = df.filter( (df['age'] > 30) | (df['department'] == 'Marketing') )
print(df_filtered.head(5)) # or just df_filtered.show()

df2 = df.where(df['age'] < 20) # same as .filter
df2.show() # Olq, 18

# add a row to DF with union
newRow = spark.createDataFrame([ (5, 'Bali',    39, 'IT') ], columns)
df3 = df.union(newRow)

# group by, aggregate
from pyspark.sql import functions as F

df_agg = df3.groupBy('department').agg(
    F.count('name'),
    F.avg('age').alias('avg_age')
)
df_agg.show()
"""
+----------+-----------+-------+
|department|count(name)|avg_age|
+----------+-----------+-------+
|        HR|          1|   28.0|
|        IT|          2|   42.0|
|   Finance|          1|   33.0|
| Marketing|          1|   18.0|
+----------+-----------+-------+
"""

# joins
salary_data = [(1, 50000), (2, 60000), (3, 55000), (4, 33000), (5, 70000), (9, 11111111)]
salary_columns = 'employee_id', 'salary'
df_salaries = spark.createDataFrame(salary_data, salary_columns)

df_combined = df3.join(df_salaries, on='employee_id', how='inner')
df_combined.show()

df3.join(df_salaries, on='employee_id', how='outer').show()
"""
+-----------+-----+----+----------+--------+
|employee_id| name| age|department|  salary|
+-----------+-----+----+----------+--------+
|          1|Elena|  28|        HR|   50000|
|          2| Ivan|  45|        IT|   60000|
|          3|Yasen|  33|   Finance|   55000|
|          4|  Olq|  18| Marketing|   33000|
|          5| Bali|  39|        IT|   70000|
|          9| NULL|NULL|      NULL|11111111|
+-----------+-----+----+----------+--------+
"""
