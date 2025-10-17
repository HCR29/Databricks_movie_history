# Databricks notebook source
# MAGIC %md
# MAGIC #### Ingesta del archivo "production_company.json"

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Paso 1 Leer el archivo JSON usando "DataframeReader" de Spark

# COMMAND ----------

dbutils.widgets.text("p_enviroment", "")
v_enviroment = dbutils.widgets.get("p_enviroment")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2024-12-16")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/commont_functions"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# COMMAND ----------

productions_companies_schema = StructType(
fields=[
StructField("companyId", IntegerType(), False),
StructField("companyName", StringType(), True),
])
                         

# COMMAND ----------

productions_companies_df = spark.read \
.schema(productions_companies_schema)\
.csv(f"{bronze_folder_path}/{v_file_date}/production_company")


# COMMAND ----------

display(productions_companies_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Paso 2 - Renombrar las columnas y añadir nuevas columnas
# MAGIC 1. "companyId" renombrar a "company_id" , "companyName" a "company_name"
# MAGIC 2. Agregar columnas "ingestion_date" y "enviroment"
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import col, concat, current_timestamp, lit

# COMMAND ----------

produc_company_with_columns_df =add_ingestion_date(productions_companies_df)\
                  .withColumn("enviroment", lit(v_enviroment))\
                    .withColumn("file_date", lit(v_file_date))\
                  .withColumnRenamed("companyId", "company_id") \
                .withColumnRenamed("companyName", "company_name")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Paso 3 - Escribir la salida en formato parquet

# COMMAND ----------

#produc_company_with_columns_df.write.mode("overwrite").format("parquet").saveAsTable("movie_silver.productions_companies")

# COMMAND ----------

merge_condition = 'tgt.company_id = src.company_id AND tgt.file_date = src.file_date'
merge_delta_lake(produc_company_with_columns_df, "movie_silver", "productions_companies", silver_folder_path, merge_condition, "file_date")


# COMMAND ----------

# MAGIC %sql
# MAGIC select file_date, count(1)
# MAGIC from movie_silver.productions_companies
# MAGIC group by file_date 

# COMMAND ----------

dbutils.notebook.exit("success")
