from pyspark.sql import SparkSession as ss, functions as f, window as w

spark = ss.builder.appName("Superstore Incremental").getOrCreate()
df = spark.read.option("header", True).option("inferSchema", True).csv("s3://de-sales-analytics/raw/sales/*.csv")
processed_date = spark.read.text("s3://de-sales-analytics/processed/bookmarks/")
latest_date = processed_date.first()[0]
relevant_data = df.select(f.col("Order_ID"), f.col("Order_Date"), f.col("Customer_ID"), f.col("Sales"))
format_order_date = relevant_data.withColumn("order_date", f.to_date("Order_Date", "M/d/yyyy"))
# format_order_date.show()
latest_order_date = format_order_date.filter(f.col("order_date") > latest_date)
if latest_order_date.count() == 0:
    print("No new data is found")
    spark.stop()
    exit(0)

silver_df = latest_order_date.select(
    "Order_ID",
    "order_date",
    "Customer_ID",
    "Sales"
)
silver_df.write.mode("append").parquet("s3://de-sales-analytics/silver/sales/")
silver_all = spark.read.parquet("s3://de-sales-analytics/silver/sales/")

gold_df = silver_all.groupBy("order_date").agg(f.sum("Sales").alias("daily_revenue"))
gold_df.write.mode("overwrite").parquet("s3://de-sales-analytics/gold/daily_sales/")
gold_all = spark.read.parquet("s3://de-sales-analytics/gold/daily_sales/")

max_date = latest_order_date.agg(f.max(f.col("order_date")).alias("latest_processed_date"))
# max_date.show()
max_date.select(f.date_format("latest_processed_date", "yyyy-MM-dd")). \
    write.mode("overwrite").text("s3://de-sales-analytics/processed/bookmarks/")
latest_order_date.show()
spark.stop()

# CSV (Kaggle)
#    ↓
# S3 (Raw layer)
#    ↓
# EMR + PySpark (Incremental ETL)
#    ↓
# S3 (Silver Parquet)
#    ↓
# S3 (Gold Parquet)
#    ↓
# Athena (SQL engine)
#    ↓
# QuickSight (Dashboard)

# Built an end-to-end data lake pipeline using Amazon S3, EMR (PySpark), Athena, and QuickSight to process raw sales data into analytics-ready datasets.

# Implemented incremental data processing using a bookmark (high-water-mark) approach to load only new records into Silver and Gold Parquet layers.

# Created a business-ready BI dashboard in Amazon QuickSight on top of Athena to visualize daily sales revenue trends from the Gold layer.