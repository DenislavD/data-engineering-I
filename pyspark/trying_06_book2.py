# window functions intro

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('my6').getOrCreate()

df = spark.read.csv('zorro.csv', header=True, inferSchema=True, dateFormat='dd-MM-yyyy')
df.show()

processed_data = df.filter('Rating < 5')
processed_data.createTempView('low_rating')
results = spark.sql("SELECT * FROM low_rating")
results.show()

# Window functions intro (widely used)
from pyspark.sql import Window
from pyspark.sql.functions import col, count

window_spec = Window.partitionBy('Country').orderBy('Power')
# try orderBy('Date') to see that the range frame includes all peers (same partition and order)
# fix:
# window_spec = Window.partitionBy('Country').orderBy('Date').rowsBetween(Window.unboundedPreceding, Window.currentRow)
df2 = df.withColumn('count', count('Hero').over(window_spec) )
df2.show()

raise SystemExit(1) # normal exit

# base syntax:
function().over( Window.partitionBy('col1', 'col2').orderBy('col2').rowsBetween(50, 100) )

