# Databricks notebook source
# MAGIC %md
# MAGIC #### Ingesta del archivo "movie_languages.json"

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

movie_languages_schema = StructType(fields=[
StructField("movieId", IntegerType(), False),
StructField("languageId", IntegerType(), True),
StructField("languageRoleId", IntegerType(), True),
])
                         

# COMMAND ----------

movie_languages_df = spark.read \
.schema(movie_languages_schema)\
.option("multiLine", "true")\
.json(f"{bronze_folder_path}/{v_file_date}/movie_language")


# COMMAND ----------

display(movie_languages_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Paso 2 - Renombrar las columnas y añadir nuevas columnas
# MAGIC 1. "movieId" renombrar a "movie_id" , "companyId" a "company_id"
# MAGIC 2. Agregar columnas "ingestion_date" y "enviroment"
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import col, concat, current_timestamp, lit

# COMMAND ----------

movie_language_with_columns_df =add_ingestion_date(movie_languages_df)\
                  .withColumn("enviroment", lit(v_enviroment))\
                    .withColumn("file_date", lit(v_file_date))\
                .withColumnRenamed("movieId", "movie_id") \
                .withColumnRenamed("languageId", "language_id")
                

# COMMAND ----------

## Eliminar columna "languajeRoleId"

# COMMAND ----------

from pyspark.sql.functions import col


# COMMAND ----------

movie_final = movie_language_with_columns_df.drop(col("languageRoleId") )

# COMMAND ----------

# MAGIC %md
# MAGIC #### Paso 3 - Escribir la salida en formato parquet

# COMMAND ----------

# overwrite_partition(movies_final, "movie_silver", "movies", "file_date")

# COMMAND ----------

merge_condition = 'tgt.movie_id = src.movie_id AND tgt.language_id = src.language_id AND tgt.file_date = src.file_date'
merge_delta_lake(movie_language_with_columns_df, "movie_silver", "movies_languages", silver_folder_path, merge_condition, "file_date")


# COMMAND ----------

# MAGIC %sql
# MAGIC select file_date, count(1)
# MAGIC from movie_silver.movies_languages
# MAGIC group by file_date 

# COMMAND ----------

dbutils.notebook.exit("success")
