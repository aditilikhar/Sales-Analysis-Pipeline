# Goal: Analyze sales data to gain insights into revenue, sales trends,
# and customer behavior.

#import packages
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

#create SparkSession
spark=SparkSession.builder\
    .getOrCreate()

#load data and create dataframe
orders_path='/home/aditi/Downloads/Projects/orders.csv'
customers_path='/home/aditi/Downloads/Projects/customers.csv'
products_path='/home/aditi/Downloads/Projects/products.csv'

orders = spark.read\
    .option('header','true') \
    .option('inferSchema','true')\
    .csv(orders_path)

customers = spark.read\
    .option('header','true') \
    .option('inferSchema','true')\
    .csv(customers_path)

products = spark.read\
    .option('header','true') \
    .option('inferSchema','true')\
    .csv(products_path)

#test dataframe
orders.printSchema()
orders.show(n=10,truncate=False)
print('-----------------------------------------------------------------------------------')
customers.printSchema()
customers.show(n=10,truncate=False)
print('-----------------------------------------------------------------------------------')
products.printSchema()
products.show(n=10,truncate=False)
print('-----------------------------------------------------------------------------------')


orders=orders.withColumnRenamed("ID","OrderID")
customers=customers.withColumnRenamed("ID","CustomerID")
products=products.withColumnRenamed("ID","ProductID")

orders = orders.withColumnRenamed("Created At", "OrderCreatedAt")
customers = customers.withColumnRenamed("Created At", "CustomerCreatedAt")
products = products.withColumnRenamed("Created At", "ProductCreatedAt")


#join DataFrames
joined_df = orders\
    .join(customers,orders['User ID']==customers['CustomerID'],'inner')\
    .join(products,orders['Product ID']==products['ProductID'],'inner')\

joined_df.printSchema()
joined_df.show(n=5)

#Calculate Total Revenue
total_revenue = joined_df.agg(sum("Total").alias("TotalRevenue")).collect()[0]["TotalRevenue"]
print(f"Total Revenue: {total_revenue}")

#calculate Sales by Category
# sales_by_category = joined_df.groupBy("Category")\
#     .agg(sum("Total").alias("TotalSales"))

#calculate Sales by Category
sales_by_category = joined_df.groupBy("Category")\
    .agg(sum("Total").alias("TotalSales"))\
    .select("Category", "TotalSales")
sales_by_category.show()

#Customer Segmentation By Total Spending
customer_segmentation = joined_df.groupBy("User ID","Name")\
    .agg(sum("Total").alias("Total Spending"),count("OrderID").alias("Purchase Frequency"))\
    .orderBy(desc("Purchase Frequency"))

customer_segmentation.show(n=5)

# Time Series Analysis for Monthly Sales Trends
monthly_sales_trends = joined_df.groupBy(year("OrderCreatedAt").alias("Year"),
                                          month("OrderCreatedAt").alias("Month"))\
                                .agg(sum("Total").alias("TotalSales"))\
                                .orderBy("Year", "Month", ascending=True)
monthly_sales_trends.show(n=5)


# Save Results
sales_by_category.write \
    .mode("overwrite") \
    .option('header',"true")\
    .csv("./results/sales_by_category.csv")

customer_segmentation.write \
    .mode("overwrite") \
    .option('header',"true")\
    .csv("./results/customer_segmentation.csv")

monthly_sales_trends.write\
    .mode("overwrite")\
    .option("header", "true")\
    .csv("./results/monthly_sales_trends.csv")

spark.stop()