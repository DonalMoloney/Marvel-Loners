"""
This script processes customer order datasets to compute and display the total amount
each customer has spent. The data is aggregated by customer ID, and the resulting
amounts are sorted in ascending order. The top 10 customers with the lowest spending
are then displayed.

Dependencies:
- PySpark

Usage:
- Ensure that PySpark is installed and correctly configured.
- Modify the path "file:///SparkCourse/customer-orders.csv" to point to the appropriate data file.
- Run the script to view the total amount spent by each customer.
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType

# Initialize SparkSession
spark_session = SparkSession.builder.appName("TotalSpentByCustomer").master("local[*]").getOrCreate()

# Define the schema for customer order data
order_schema = StructType([
    StructField("customer_id", IntegerType(), True),
    StructField("product_id", IntegerType(), True),
    StructField("amount", FloatType(), True)
])

# Read the customer order data from CSV into a DataFrame
order_df = spark_session.read.schema(order_schema).csv("file:///SparkCourse/customer-orders.csv")

# Compute the total amount spent by each customer and round to 2 decimal places
total_spent_by_customer = order_df.groupBy("customer_id").agg(func.round(func.sum("amount"), 2).alias("total_spent"))

# Sort the result by the total spent in ascending order and show the top 10 results
sorted_spent_by_customer = total_spent_by_customer.sort("total_spent")
sorted_spent_by_customer.limit(10).show()

spark_session.stop()
