from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("DataCleaningExample").getOrCreate()

# Load orders data
# orders_df = spark.read.csv("/mnt/data/orders.csv", header=True, inferSchema=True)

# Load products data
products_df = spark.read.csv("products1.csv", header=True, inferSchema=True)

# Show the loaded data
# orders_df.show(5)
products_df.show(5)

from pyspark.sql.functions import lit

# Introduce missing values in the orders data
# unclean_orders_df = orders_df.withColumn("Total", lit(None).cast("double")).union(orders_df.limit(10))

# Introduce duplicate rows
# unclean_orders_df = unclean_orders_df.union(unclean_orders_df.limit(5))

# Introduce inconsistent data types (e.g., strings in numeric columns)
unclean_products_df = products_df.withColumn("Price", lit("N/A").cast("string")).union(products_df.limit(10))

# Show the uncleaned data
# unclean_orders_df.show(5)
unclean_products_df.show(15)

