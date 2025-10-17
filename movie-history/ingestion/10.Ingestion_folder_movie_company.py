# Databricks notebook source
# MAGIC %md
# MAGIC #### Ingesta del archivo "movie_company.json"

# COMMAND ----------

dbutils.widgets.text("p_enviroment", "")
v_enviroment = dbutils.widgets.get("p_enviroment")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2024-12-30")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/commont_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Paso 1 Leer el archivo JSON usando "DataframeReader" de Spark

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# COMMAND ----------

movie_companies_schema = StructType(
fields=[
StructField("movieId", IntegerType(), False),
StructField("companyId", IntegerType(), True),
])
                         

# COMMAND ----------

movie_companies_df = spark.read \
.schema(movie_companies_schema)\
.csv(f"{bronze_folder_path}/{v_file_date}/movie_company")


# COMMAND ----------

display(movie_companies_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Paso 2 - Renombrar las columnas y añadir nuevas columnas
# MAGIC 1. "movieId" renombrar a "movie_id" , "companyId" a "company_id"
# MAGIC 2. Agregar columnas "ingestion_date" y "enviroment"
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import col, concat, current_timestamp, lit

# COMMAND ----------

movie_company_with_columns_df = add_ingestion_date(movie_companies_df)\
                  .withColumn("enviroment", lit(v_enviroment))\
                    .withColumn("file_date", lit(v_file_date))\
                .withColumnRenamed("movieId", "movie_id") \
                .withColumnRenamed("companyId", "company_id")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Paso 3 - Escribir la salida en formato parquet

# COMMAND ----------

merge_condition = 'tgt.movie_id = src.movie_id AND tgt.company_id = src.company_id AND tgt.file_date = src.file_date'
merge_delta_lake(movie_company_with_columns_df, "movie_silver", "movies_companies", silver_folder_path, merge_condition, "file_date")



# COMMAND ----------

# MAGIC %sql
# MAGIC select file_date, count(1)
# MAGIC from movie_silver.movies_companies
# MAGIC group by file_date 

# COMMAND ----------

dbutils.notebook.exit("success")
