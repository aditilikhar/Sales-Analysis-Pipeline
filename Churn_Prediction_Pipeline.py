from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import DateType
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator

# Create SparkSession
spark = SparkSession.builder.getOrCreate()

# Load data
orders_path = '/home/aditi/Downloads/Projects/orders.csv'
customers_path = '/home/aditi/Downloads/Projects/customers.csv'

orders = spark.read.option('header', 'true').option('inferSchema', 'true').csv(orders_path)
customers = spark.read.option('header', 'true').option('inferSchema', 'true').csv(customers_path)

# Rename columns for clarity
orders = orders.withColumnRenamed("ID", "OrderID") \
               .withColumnRenamed("Created At", "OrderCreatedAt") \
               .withColumnRenamed("User ID", "UserID")
customers = customers.withColumnRenamed("ID", "CustomerID") \
                     .withColumnRenamed("Created At", "CustomerCreatedAt")

# Convert date columns to DateType
orders = orders.withColumn("OrderCreatedAt", col("OrderCreatedAt").cast(DateType()))
customers = customers.withColumn("CustomerCreatedAt", col("CustomerCreatedAt").cast(DateType()))

# Join DataFrames
joined_df = orders.join(customers, orders["UserID"] == customers["CustomerID"], "inner")

# Feature Engineering
feature_df = joined_df.groupBy("UserID") \
    .agg(
        sum("Total").alias("TotalSpend"),
        count("OrderID").alias("PurchaseFrequency"),
        max("OrderCreatedAt").alias("LastPurchaseDate")
    )

# Calculate recency
current_date = current_date()
feature_df = feature_df.withColumn("Recency", datediff(current_date, col("LastPurchaseDate")))

feature_df.show()

# Define churn label
feature_df = feature_df.withColumn(
    "Churned",
    when(col("Recency") > 180, 1).otherwise(0)
)

feature_df.show()

# Prepare feature vector
assembler = VectorAssembler(
    inputCols=["TotalSpend", "PurchaseFrequency", "Recency"],
    outputCol="features"
)
assembled_df = assembler.transform(feature_df)

# Split data into training and test sets
(training_data, test_data) = assembled_df.randomSplit([0.8, 0.2], seed=1234)

# Train Logistic Regression model
lr = LogisticRegression(featuresCol="features", labelCol="Churned")
model = lr.fit(training_data)

# Make predictions
predictions = model.transform(test_data)
predictions.select("UserID", "Churned", "prediction").show()

# Evaluate the model
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

# Initialize evaluator for accuracy
accuracy_evaluator = MulticlassClassificationEvaluator(labelCol="Churned", predictionCol="prediction", metricName="accuracy")
accuracy = accuracy_evaluator.evaluate(predictions)
print(f"Accuracy: {accuracy}")

# Initialize evaluator for precision
precision_evaluator = MulticlassClassificationEvaluator(labelCol="Churned", predictionCol="prediction", metricName="precisionByLabel")
precision = precision_evaluator.evaluate(predictions)
print(f"Precision: {precision}")

# Initialize evaluator for recall
recall_evaluator = MulticlassClassificationEvaluator(labelCol="Churned", predictionCol="prediction", metricName="recallByLabel")
recall = recall_evaluator.evaluate(predictions)
print(f"Recall: {recall}")

# Initialize evaluator for F1 score
f1_evaluator = MulticlassClassificationEvaluator(labelCol="Churned", predictionCol="prediction", metricName="f1")
f1 = f1_evaluator.evaluate(predictions)
print(f"F1 Score: {f1}")


# Evaluate the model
# evaluator = BinaryClassificationEvaluator(labelCol="Churned")
# accuracy = evaluator.evaluate(predictions, {evaluator.metricName: "accuracy"})
# precision = evaluator.evaluate(predictions, {evaluator.metricName: "precisionByLabel"})
# recall = evaluator.evaluate(predictions, {evaluator.metricName: "recallByLabel"})
# f1 = evaluator.evaluate(predictions, {evaluator.metricName: "f1"})
#
# print(f"Accuracy: {accuracy}")
# print(f"Precision: {precision}")
# print(f"Recall: {recall}")
# print(f"F1 Score: {f1}")


# Save predictions
predictions.select("UserID", "Churned", "prediction") \
    .write.mode("overwrite") \
    .option("header", "true") \
    .csv("./results/churn_predictions.csv")

spark.stop()
