# Databricks notebook source
# MAGIC %md
# MAGIC #### Ingesta del archivo "production_country.json"

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Paso 1 Leer el archivo JSON usando "DataframeReader" de Spark

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

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# COMMAND ----------

productions_countries_schema = StructType(fields=[
StructField("movieId", IntegerType(), False),
StructField("countryId", IntegerType(), True)
])
                         

# COMMAND ----------

productions_countries_df = spark.read \
                        .schema(productions_countries_schema)\
                        .option("multiLine", "true")\
                        .json(f"{bronze_folder_path}/{v_file_date}/production_country")


# COMMAND ----------

display(productions_countries_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Paso 2 - Renombrar las columnas y añadir nuevas columnas
# MAGIC 1. "movieId" renombrar a "movie_id" , "countryId" a "country_id"
# MAGIC 2. Agregar columnas "ingestion_date" y "enviroment"
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import col, concat, current_timestamp, lit

# COMMAND ----------

productions_countries_final_df =add_ingestion_date(productions_countries_df)\
                  .withColumn("enviroment", lit(v_enviroment))\
                    .withColumn("file_date", lit(v_file_date))\
                .withColumnRenamed("movieId", "movie_id") \
                .withColumnRenamed("countryId", "country_id")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Paso 3 - Escribir la salida en formato parquet

# COMMAND ----------

merge_condition = 'tgt.movie_id = src.movie_id AND tgt.country_id = src.country_id AND tgt.file_date = src.file_date'
merge_delta_lake(productions_countries_final_df, "movie_silver", "productions_countries", silver_folder_path, merge_condition, "file_date")

# COMMAND ----------

# MAGIC %sql
# MAGIC select file_date, count(1)
# MAGIC from movie_silver.productions_countries
# MAGIC group by file_date 

# COMMAND ----------

dbutils.notebook.exit("success")
