from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Create SparkSession
spark = SparkSession.builder.getOrCreate()

# Load data
orders_path = '/home/aditi/Downloads/Projects/orders.csv'
orders = spark.read.option('header', 'true').option('inferSchema', 'true').csv(orders_path)

# Add a column to indicate if the order is discounted or not
orders_with_discount_flag = orders.withColumn("IsDiscounted", when(col("Discount").isNotNull(), 1).otherwise(0))

# Analyze Sales Impact
# Replace 'OrderID' with 'ID'
sales_impact_df = orders_with_discount_flag.groupBy("IsDiscounted")\
    .agg(
        sum("Total").alias("TotalSales"),
        count("ID").alias("OrderCount")  # Use 'ID' instead of 'OrderID'
    )
sales_impact_df.show()

# Analyze Customer Behavior
# Replace 'OrderID' with 'ID'
repeat_purchase_df = orders_with_discount_flag.groupBy("User ID", "IsDiscounted")\
    .agg(countDistinct("ID").alias("NumberOfOrders"))\
    .groupBy("IsDiscounted")\
    .agg(
        avg("NumberOfOrders").alias("AverageOrdersPerCustomer"),
        countDistinct("User ID").alias("UniqueCustomers")
    )
repeat_purchase_df.show()

# Save results
sales_impact_df.write.mode("overwrite")\
    .option("header", "true")\
    .csv("./results/sales_impact.csv")

repeat_purchase_df.write.mode("overwrite")\
    .option("header", "true")\
    .csv("./results/customer_behavior.csv")

spark.stop()
