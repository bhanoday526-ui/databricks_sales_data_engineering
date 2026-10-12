# Databricks notebook source
df = spark.read.option("header","true").option("inferSchema","true").csv("/Volumes/workspace/sales_project/raw_data/sales_project_raw.csv")
display(df)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

print ("Total rows:",df.count())
print("Total columns:",len(df.columns))

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze Layer

# COMMAND ----------

bronze_path = "/Volumes/workspace/sales_project/bronze"
print(bronze_path)

# COMMAND ----------

display(dbutils.fs.ls("/Volumes/workspace/sales_project/raw_data"))

# COMMAND ----------

spark.sql("CREATE VOLUME IF NOT EXISTS workspace.sales_project.bronze")
dbutils.fs.mkdirs("/Volumes/workspace/sales_project/bronze")

# COMMAND ----------

df.write.format("delta").mode("overwrite").save(bronze_path)


# COMMAND ----------

bronze_df = spark.read.format("delta").load(bronze_path)
display(bronze_df)


# COMMAND ----------

print("Bronze rows:",bronze_df.count())


# COMMAND ----------

bronze_df.write.format("delta").mode("overwrite").saveAsTable("workspace.sales_project.bronze_sales")

# COMMAND ----------

spark.sql("""SELECT* FROM workspace.sales_project.bronze_sales LIMIT 10""").display()

# COMMAND ----------

bronze_df=spark.table("workspace.sales_project.bronze_sales")

# COMMAND ----------

print("Total rows:",bronze_df.count())
print("Distinct rows:",bronze_df.distinct().count())

# COMMAND ----------

silver_df = bronze_df.dropDuplicates()



# COMMAND ----------

print("Bronze rows:",bronze_df.count())
print("Silver rows:",silver_df.count())


# COMMAND ----------

from pyspark.sql.functions import col
silver_df.filter(col("customer_name").isNull()
).display()

# COMMAND ----------

from pyspark.sql.functions import coalesce, lit
silver_df = silver_df.withColumn("customer_name",coalesce(col("customer_name"),lit("Unknown customer")))

# COMMAND ----------

from pyspark.sql.functions import initcap
silver_df = silver_df.withColumn("region",initcap(col("region")))
display(silver_df)


# COMMAND ----------

from pyspark.sql.functions import trim
silver_df = silver_df.withColumn("customer_name",trim(col("customer_name")))
display(silver_df)


# COMMAND ----------

silver_df.printSchema()


# COMMAND ----------

print("Silver rows:",silver_df.count())

# COMMAND ----------

silver_path= "/Volumes/workspace/sales_project/silver"

# COMMAND ----------

spark.sql("CREATE VOLUME IF NOT EXISTS workspace.sales_project.silver")
silver_df.write.format("delta").mode("overwrite").save(silver_path)

# COMMAND ----------



# COMMAND ----------

silver_df.write.format("delta").mode("overwrite").saveAsTable("workspace.sales_project.silver_sales")

# COMMAND ----------

spark.sql("SELECT*FROM workspace.sales_project.silver_sales LIMIT 10").display()

# COMMAND ----------

silver_df= spark.table("workspace.sales_project.silver_sales")
display(silver_df)

# COMMAND ----------

silver_df.columns

# COMMAND ----------

from pyspark.sql.functions import sum,countDistinct
gold_sales_summary = silver_df.groupBy("category").agg(sum("sales").alias("total_sales"),countDistinct("order_id").alias("total_orders"))
display(gold_sales_summary)

# COMMAND ----------

gold_path= "/Volumes/workspace/sales_project/gold"

# COMMAND ----------

spark.sql("CREATE VOLUME IF NOT EXISTS workspace.sales_project.gold")
gold_sales_summary.write.format("delta").mode("overwrite").save(gold_path)

# COMMAND ----------

gold_sales_summary.write.format("delta").mode("overwrite").saveAsTable("workspace.sales_project.gold_sales_summary")

# COMMAND ----------

spark.sql("SELECT*FROM workspace.sales_project.gold_sales_summary ORDER BY total_sales DESC ").display()

# COMMAND ----------

