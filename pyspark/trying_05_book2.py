# to_date, other transformations, cross join, read/write, UDFs

from pyspark import StorageLevel
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import *

spark = SparkSession.builder.appName('my5').getOrCreate()

df = spark.read.csv('zorro.csv', header=True, inferSchema=True)
df.printSchema()
# |      Zorro|   Swordfighting|Legend of Zorro|Zerrikania|01-01-1999|    10|
df2 = df.withColumn('Date', F.to_date('Date', 'dd-MM-yyyy')) # converts column to DateType
# |      Zorro|   Swordfighting|Legend of Zorro|Zerrikania|1999-01-01|    10|
df2.printSchema()

df2.dropDuplicates()
df2.dropna() # drops rows with ANY NULL value

df2.persist(StorageLevel.MEMORY_ONLY_2) # 2 copies, up to 3 for DISK_ONLY. See also .cache()

df2.createOrReplaceTempView('df2')
spark.sql("SELECT COUNT(*) FROM df2") # 8, total rows
spark.sql("SELECT COUNT(Power) FROM df2") # 6, 2 rows have NULLs in this column

df2.sort('Movie', ascending=False).show() # same as orderBy

df.agg({'Rating': 'avg', }).show()
df.agg(F.avg('Rating')).show() # same
df.select(F.avg('Rating')).show() # same

df.groupBy('Country').avg().show() # gets the numeric columns automatically

# cross join
dfx = spark.createDataFrame([('US', 512), ('France', 33), ('Germany', 0)])
df_cross = df2.join(dfx) # or how='cross' with no on=
df_cross.show(50)

df_cross.write.csv('cross_joined', mode='overwrite', header=True)
# writes it in many files, partitioned by Country? hmm..
df_cross.write.format('csv').option('header', True).save('cross_joined.csv', mode='overwrite')
# see also format='delta'

# User-Defined Functions
from pyspark.sql.functions import udf

# create the function
def udf_is_odd(number) -> bool: # pyspark warning: can't infer type from type hints, lol
    if not isinstance(number, int): return True
    if number == 0: return False
    return bool(number % 2)

# register the UDF and its return value type with Spark
is_odd = udf(udf_is_odd, BooleanType())

df2 = df2.withColumn('odd?', is_odd(F.col('Rating')))
df2.show()

df_cross.explain() # Catalyst Optimizer's steps - can't read it yet

# cleanup (not really needed, just as a category)
df2.unpersist() # remove from cache
df.coalesce(1) # reduce number of partitions, also see .repartition(n)
