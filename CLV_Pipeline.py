# Import packages
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Create SparkSession
spark = SparkSession.builder \
    .appName("Customer Lifetime Value Calculation") \
    .getOrCreate()

# Load data and create DataFrames
orders_path = '/home/aditi/Downloads/Projects/orders.csv'
customers_path = '/home/aditi/Downloads/Projects/customers.csv'
products_path = '/home/aditi/Downloads/Projects/products.csv'

orders = spark.read \
    .option('header', 'true') \
    .option('inferSchema', 'true') \
    .csv(orders_path)

customers = spark.read \
    .option('header', 'true') \
    .option('inferSchema', 'true') \
    .csv(customers_path)

products = spark.read \
    .option('header', 'true') \
    .option('inferSchema', 'true') \
    .csv(products_path)

# Rename columns for consistency
orders = orders.withColumnRenamed("ID", "OrderID") \
               .withColumnRenamed("Created At", "OrderCreatedAt")

customers = customers.withColumnRenamed("ID", "CustomerID") \
                     .withColumnRenamed("Created At", "CustomerCreatedAt")

products = products.withColumnRenamed("ID", "ProductID") \
                   .withColumnRenamed("Created At", "ProductCreatedAt")

# Join DataFrames
joined_df = orders \
    .join(customers, orders['OrderID'] == customers['CustomerID'], 'inner')

# Calculate Lifetime Value (CLV)
lifetime_value_df = joined_df \
    .groupBy("CustomerID") \
    .agg(sum("Total").alias("LifetimeValue")) \
    .orderBy(desc("LifetimeValue"))

# Calculate Order Count
order_count_df = joined_df \
    .groupBy("CustomerID") \
    .agg(count("OrderID").alias("OrderCount"))

# Calculate Average Order Value (AOV)
aov_df = lifetime_value_df \
    .join(order_count_df, "CustomerID") \
    .withColumn('AverageOrderValue', col('LifetimeValue') / col('OrderCount'))

# Calculate CLV
clv_df = aov_df \
    .join(order_count_df.withColumnRenamed("OrderCount", "PurchaseFrequency"), "CustomerID") \
    .withColumn("CLV", col("AverageOrderValue") * col("PurchaseFrequency"))

# Show the result
clv_df.select("CustomerID", "CLV").show()

# Save the result
clv_df.select("CustomerID", "CLV") \
    .write.mode("overwrite") \
    .option("header", "true") \
    .csv("./results/customer_lifetime_value.csv")

spark.stop()
