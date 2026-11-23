# Functions, casting and SQL temp view

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import *

spark = SparkSession.builder.appName('my4').getOrCreate()
sc = spark.sparkContext

rdd1 = sc.parallelize([ (1,2,3), (4,5,6), (7,8,9) ])
df1 = spark.createDataFrame(rdd1, schema=['num1', 'num2', 'num3'])
df1.show(1, vertical=True) # shows each column in a separate row
# -RECORD 0---
#  num1 | 1
#  num2 | 2
#  num3 | 3

pandas_df = df1.toPandas()

df2 = df1.withColumn('str4', F.lit('stringggg'))
df2 = df2.withColumnRenamed('str4', 'Fixed String')
df2.printSchema()

schema = StructType([
    StructField('num1', IntegerType()),
    StructField('num2', IntegerType()),
    StructField('num3', IntegerType()),
    StructField('str4',StringType(), True)
])
dfx = spark.createDataFrame([ (11,11,11, None) ], schema) # testing nullable
df_union = df2.union(dfx)
df_union.show()
# +----+----+----+------------+
# |num1|num2|num3|Fixed String|
# +----+----+----+------------+
# |   1|   2|   3|   stringggg|
# |   4|   5|   6|   stringggg|
# |   7|   8|   9|   stringggg|
# |  11|  11|  11|        NULL|
# +----+----+----+------------+

df_union.select('Fixed String').distinct().show()
df_union.select( F.countDistinct(F.col('Fixed String')).alias('Total Unique') ).show()
# +------------+
# |Total Unique|
# +------------+
# |           1|
# +------------+

df2 = df2.withColumn( 'Fixed String', F.upper(F.col('Fixed String')) )
df2.show()

list_ = [1, 7]
filtered_df = df2.filter(df2.num1.isin(list_))
filtered_df = df2.filter(df2['num1'].isin(list_))
filtered_df = df2.filter(F.col('num1').isin(list_))

# Type casting
df2.num3.cast(StringType())
df_union.createOrReplaceTempView('cast_example')
df4 = spark.sql("SELECT DOUBLE(num1), STRING(num3) FROM cast_example")
df4.printSchema()
