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

raise SystemExit(1)
# base syntax:
function().over( Window.partitionBy('col1', 'col2').orderBy('col2').rowsBetween(a, b) )


# PRACTICAL ASSIGNMENT
orders_df = spark.read.csv('z_orders.csv', header=True, inferSchema=True)
customers_df = spark.createDataFrame([(101, 'Ivan', 'Bulgaria'), (102, 'Maria', 'Greece'), 
    (103, 'Georgi', 'Bulgaria'), (105, 'Elena', 'Romania'), ], schema=['customer_id', 'name', 'country'])

"""
1. Replace null quantity with 1.
2. Add a column total_amount = quantity * unit_price.
3. Add a column order_month with the month number taken from order_date (it's a string, so convert it first).
4. Add a column order_size that is "large" when total_amount >= 500, otherwise "small".
5. Join with customers_df so that only orders with a matching customer are kept.
6. Group by country and calculate:
    num_orders: the number of orders
    revenue: the sum of total_amount, rounded to 2 decimals
    large_orders: the number of orders where order_size == "large"
7. Sort by revenue, highest first.
8. Write the result as Parquet to /tmp/output/country_sales, overwriting anything already there.
"""

from pyspark.sql import functions as F

result_df = (
    orders_df
    .fillna(1, subset=['quantity'])
    .withColumn('total_amount', F.col('quantity') * F.col('unit_price'))
    .withColumn('order_month', F.month(F.to_date('order_date', 'yyyy-MM-dd')))
    .withColumn('order_size',
                F.when(F.col('total_amount') >= 500, 'large').otherwise('small'))
    .join(customers_df, on='customer_id', how='inner')
    .groupBy('country')
    .agg(
        F.count('order_id').alias('num_orders'),
        F.round(F.sum('total_amount').cast('float'), 2).alias('revenue'),
        F.sum(F.when(F.col('order_size') == 'large', 1).otherwise(0)).alias('large_orders'),
    )
    .orderBy(F.col('revenue').desc())
)

result_df.write.mode('overwrite').parquet('country_sales')
result_df.show()