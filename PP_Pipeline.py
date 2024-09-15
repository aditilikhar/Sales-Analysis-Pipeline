# Import packages
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Create SparkSession
spark = SparkSession.builder \
    .appName("Product Performance Evaluation") \
    .getOrCreate()

# Load data and create DataFrames
orders_path = '/home/aditi/Downloads/Projects/orders.csv'
products_path = '/home/aditi/Downloads/Projects/products.csv'

orders = spark.read \
    .option('header', 'true') \
    .option('inferSchema', 'true') \
    .csv(orders_path)

products = spark.read \
    .option('header', 'true') \
    .option('inferSchema', 'true') \
    .csv(products_path)

# Rename columns for consistency
orders = orders.withColumnRenamed("ID", "OrderID") \
               .withColumnRenamed("Product ID", "ProductID")

products = products.withColumnRenamed("ID", "ProductID")

# Ensure Category column is present if needed
if 'Category' not in products.columns:
    print("Category column is missing in products DataFrame. Please check your data.")
else:
    # Join DataFrames
    # Use alias to avoid ambiguity
    orders_alias = orders.alias("o")
    products_alias = products.alias("p")

    joined_df = orders_alias \
        .join(products_alias, orders_alias['ProductID'] == products_alias['ProductID'], 'inner') \
        .select(
            orders_alias['OrderID'],
            orders_alias['ProductID'].alias('OrderProductID'),
            orders_alias['Quantity'],
            orders_alias['Total'],
            products_alias['ProductID'].alias('ProductProductID'),
            products_alias['Title'],
            products_alias['Rating'],
            products_alias['Category']  # Ensure this is included if Category exists
        )

    # Calculate Sales per Product
    product_performance_df = joined_df \
        .groupBy("OrderProductID", "Title") \
        .agg(
            sum("Total").alias("TotalSales"),
            sum("Quantity").alias("TotalQuantitySold"),
            avg("Rating").alias("AverageRating")
        )

    # Show the calculated metrics
    product_performance_df.show()

    # Identify Top Performing Products by Sales
    top_performing_by_sales_df = product_performance_df \
        .orderBy(desc("TotalSales"))

    # Identify Top Performing Products by Ratings
    top_performing_by_ratings_df = product_performance_df \
        .orderBy(desc("AverageRating"))

    # Analyze Customer Preferences
    # Calculate total sales by product category (if category info is available in products DataFrame)
    category_sales_df = joined_df \
        .groupBy("Category") \
        .agg(sum("Total").alias("TotalSales"))

    # Show category sales metrics
    category_sales_df.show()

    # Save the results
    product_performance_df.write.mode("overwrite") \
        .option("header", "true") \
        .csv("./results/product_performance.csv")

    top_performing_by_sales_df.write.mode("overwrite") \
        .option("header", "true") \
        .csv("./results/top_performing_by_sales.csv")

    top_performing_by_ratings_df.write.mode("overwrite") \
        .option("header", "true") \
        .csv("./results/top_performing_by_ratings.csv")

    category_sales_df.write.mode("overwrite") \
        .option("header", "true") \
        .csv("./results/category_sales.csv")

# Stop the SparkSession
spark.stop()
