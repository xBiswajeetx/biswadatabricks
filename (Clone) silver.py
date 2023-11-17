# Databricks notebook source
# MAGIC %sql
# MAGIC use project;

# COMMAND ----------

from pyspark.sql.functions import *
df=spark.read.table("bronze")
df_final=df.withColumn("products",explode("products"))\
.withColumn("price",col("products.price"))\
.withColumn("product_id",col("products.product_id"))\
.withColumn("product_name",col("products.product_name"))\
.withColumn("quantity",col("products.quantity"))\
.withColumn("timestamp",col("timestamp").cast("timestamp"))\
.drop("products")
df_final.write.mode("overwrite").saveAsTable("project.silver")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from silver;

# COMMAND ----------


