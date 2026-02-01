# Databricks notebook source
# MAGIC %md
# MAGIC # BRONZE LEVEL

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# catalog = 'main'
# schema = 'default'
# volume_name = 'volumes'
# spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.{volume_name}")

# /Volumes/main/ecommerce/lakehouse_vol/Raw_DS/


# COMMAND ----------

# MAGIC %md
# MAGIC ###### Standardize raw data storage and prepare it for downstream processing.
# MAGIC ###### What You Need to Do:
# MAGIC ###### • Read raw CSV files from Raw_DS
# MAGIC ###### • Convert each dataset into Parquet format
# MAGIC ###### • Store Parquet files inside the bronze folder
# MAGIC ###### • Create one folder per dataset
# MAGIC ###### • Do NOT apply any transformations or business logic

# COMMAND ----------

# Read raw CSV files from Raw_DS and copy it bronze folder

# raw_df = spark.read.csv("/Volumes/main/ecommerce/lakehouse_vol/Raw_DS/", header=True, inferSchema=True)

files = dbutils.fs.ls("/Volumes/main/ecommerce/lakehouse_vol/Raw_DS/")
display(files)

# COMMAND ----------

# Convert each dataset into Parquet format and storing in bronze separte in folders

raw_path = "/Volumes/main/ecommerce/lakehouse_vol/Raw_DS/"
bronze_path = "/Volumes/main/ecommerce/lakehouse_vol/bronze/"

dataset_mapping = {
    "olist_customer_dataset.csv": "customer",
    "olist_order_items_dataset.csv": "order_items",
    "olist_order_payments_dataset.csv": "order_payments",
    "olist_orders_dataset.csv": "orders_dataset",
    "olist_products_dataset.csv": "products",
    "olist_sellers_dataset.csv": "sellers"
}


for file in dbutils.fs.ls(raw_path):
    if file.name in dataset_mapping:
        dataset_name = dataset_mapping[file.name]
        raw_df = spark.read.csv(raw_path + file.name, header=True, inferSchema=True)
        raw_df.write.mode("overwrite").parquet(bronze_path + dataset_name)
        


# COMMAND ----------

bronze_files = dbutils.fs.ls(bronze_path)
display(bronze_files)

# COMMAND ----------

# MAGIC %md
# MAGIC # SILVER LEVEL

# COMMAND ----------

customers_schema = StructType([
    StructField("customer_id", StringType(), True),
    StructField("customer_unique_id", StringType(), True),
    StructField("customer_zip_code_prefix", IntegerType(), True),
    StructField("customer_city", StringType(), True),
    StructField("customer_state", StringType(), True)
])

order_items = StructType([
    StructField("order_id", StringType(), True),
    StructField("order_item_id", IntegerType(), True),
    StructField("product_id", StringType(), True),
    StructField("seller_id", StringType(), True),
    StructField("shipping_limit_date", TimestampType(), True),
    StructField("price", DoubleType(), True),
    StructField("freight_value", DoubleType(), True),

])

order_payments = StructType([
    StructField("order_id", StringType(), True),
    StructField("payment_sequential", IntegerType(), True),
    StructField("payment_type", StringType(), True),
    StructField("payment_installments", IntegerType(), True),
    StructField("payment_value", DoubleType(), True)
])

orders_dataset = StructType([
    StructField("order_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("order_status", StringType(), True),
    StructField("order_purchase_timestamp", TimestampType(), True),
    StructField("order_approved_at", TimestampType(), True),
    StructField("order_delivered_carrier_date", TimestampType(), True),
    StructField("order_delivered_customer_date", TimestampType(), True),
    StructField("order_estimated_delivery_date", TimestampType(), True)
])

products = StructType([
    StructField("product_id", StringType(), True),
    StructField("product_category_name", StringType(), True),
    StructField("product_name_lenght", IntegerType(), True),
    StructField("product_description_lenght", IntegerType(), True),
    StructField("product_photos_qty", IntegerType(), True),
    StructField("product_weight_g", IntegerType(), True),
    StructField("product_length_cm", IntegerType(), True),
    StructField("product_height_cm", IntegerType(), True),
    StructField("product_width_cm", IntegerType(), True)
])

sellers = StructType([
    StructField("seller_id", StringType(), True),
    StructField("seller_zip_code_prefix", IntegerType(), True),
    StructField("seller_city", StringType(), True),
    StructField("seller_state", StringType(), True),
])


# Read files from bronze and enfore schema and datatypes
customer = spark.read.parquet(bronze_path + "customer").dropDuplicates(["customer_id"])
order_items = spark.read.parquet(bronze_path + "order_items").dropDuplicates(["order_id"])
order_payments = spark.read.parquet(bronze_path + "order_payments").dropDuplicates(["order_id"])
orders_dataset = spark.read.parquet(bronze_path + "orders_dataset").dropDuplicates(["order_id","customer_id"])
products = spark.read.parquet(bronze_path + "products").dropDuplicates(["product_id"]) 
sellers = spark.read.parquet(bronze_path + "sellers").dropDuplicates(["seller_id"])

print("customer", customer.printSchema())
display(customer.head(5))
# print("order_items", order_items.printSchema())
# print("order_payments", order_payments.printSchema())
# print("orders_dataset", orders_dataset.printSchema())
# print("products", products.printSchema())
# print("sellers", sellers.printSchema())

# COMMAND ----------

spark.read.parquet(bronze_path + "sellers").printSchema()


# COMMAND ----------


order_items = order_items.filter(
    col("price").isNotNull() & col("freight_value").isNotNull()
)

# COMMAND ----------

# def is_delta_path(bronze_path):
#     try:
#         # Check if the transaction log directory exists
#         dbutils.fs.ls(f"{bronze_path.customer}/_delta_log")
#         print("tr")
#         return True
#     except:
#         print("fa")
#         return False

# COMMAND ----------

# DBTITLE 1,Fix: Use overwrite mode for Delta writes
# Fix: Read the bronze data using Parquet format
silver_path = "/Volumes/main/ecommerce/lakehouse_vol/Silver/"

# 1. Load the DataFrame from the bronze location
df_customer = spark.read.parquet(bronze_path + "customer")

# 2. Write the DataFrame to a new location in "delta" format
df_customer.write.format("delta").mode("overwrite").save(silver_path + "customer")
order_items.write.format("delta").mode("overwrite").save(silver_path + "order_items")
order_payments.write.format("delta").mode("overwrite").save(silver_path + "order_payments")
orders_dataset.write.format("delta").mode("overwrite").save(silver_path + "orders_dataset")
products.write.format("delta").mode("overwrite").save(silver_path + "products")
sellers.write.format("delta").mode("overwrite").save(silver_path + "sellers")

# /Volumes/main/ecommerce/lakehouse_vol/bronze/

# COMMAND ----------

silver_customer = spark.read.format("delta").load("/Volumes/main/ecommerce/lakehouse_vol/Silver/customer")
silver_order_items = spark.read.format("delta").load("/Volumes/main/ecommerce/lakehouse_vol/Silver/order_items")
silver_order_payments = spark.read.format("delta").load("/Volumes/main/ecommerce/lakehouse_vol/Silver/order_payments")
silver_orders_dataset = spark.read.format("delta").load("/Volumes/main/ecommerce/lakehouse_vol/Silver/orders_dataset")
silver_products = spark.read.format("delta").load("/Volumes/main/ecommerce/lakehouse_vol/Silver/products")
silver_sellers = spark.read.format("delta").load("/Volumes/main/ecommerce/lakehouse_vol/Silver/sellers")


# COMMAND ----------

fact_sales = silver_order_items.alias("oi") \
    .join(silver_orders_dataset.alias("o"), col("oi.order_id") == col("o.order_id")) \
    .join(silver_customer.alias("c"), col("o.customer_id") == col("c.customer_id")) \
    .join(silver_products.alias("p"), col("oi.product_id") == col("p.product_id")) \
    .join(silver_order_payments.alias("pay"), col("oi.order_id") == col("pay.order_id"), "left") \
    .select(
        # Identifiers
        col("oi.order_id"),
        col("oi.order_item_id"),
        col("c.customer_id"),
        col("oi.product_id"),

        # Time
        col("o.order_purchase_timestamp"),
        to_date(col("o.order_purchase_timestamp")).alias("order_date"),

        # Measures
        col("oi.price"),
        col("oi.freight_value"),
        (col("oi.price") + col("oi.freight_value")).alias("revenue"),

        # Dimensions
        col("c.customer_state"),
        col("p.product_category_name"),

        # Optional
        col("pay.payment_value"),
        col("o.order_status"),
        current_date().alias("load_date")
    )
display(fact_sales)


# COMMAND ----------

silver_path = "/Volumes/main/ecommerce/lakehouse_vol/Silver/"
fact_sales.write.format("delta").mode("overwrite").save(silver_path + "fact_sales")
display(dbutils.fs.ls(silver_path + "fact_sales"))


# COMMAND ----------

silver_path = "/Volumes/main/ecommerce/lakehouse_vol/Silver/"
fact_sales = spark.read.format("delta").load(silver_path + "fact_sales")
row_count = fact_sales.count()
print(f"Number of rows in fact_sales: {row_count}")


# COMMAND ----------

null_checks = fact_sales.filter(
    col("order_id").isNull() |
    col("order_item_id").isNull() |
    col("customer_id").isNull() |
    col("product_id").isNull()
).count()
print(f"Number of nulls in fact_sales: {null_checks}")

# COMMAND ----------

invalid_revenue = fact_sales.filter(
    (col("revenue") != col("price") + col("freight_value")) |
    (col("revenue") < 0)
).count()
print(invalid_revenue)


# COMMAND ----------

duplicate_check = fact_sales.groupBy("order_id", "order_item_id") \
    .count().filter("count > 1").count()
if duplicate_check > 0:
    print(f"Duplicate records found: {duplicate_check}")
else:
    print("No duplicate records found.")


# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY '/Volumes/main/ecommerce/lakehouse_vol/Silver/fact_sales'

# COMMAND ----------

# DBTITLE 1,Untitled
old_df = spark.read.format("delta") \
    .option("versionAsOf", 0) \
    .load("/Volumes/main/ecommerce/lakehouse_vol/Silver/fact_sales")

display(old_df.head(10))

# COMMAND ----------

