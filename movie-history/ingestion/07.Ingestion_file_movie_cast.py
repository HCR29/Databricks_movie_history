# Databricks notebook source
# MAGIC %md
# MAGIC #### Ingesta del archivo "movie_cast.json"

# COMMAND ----------

dbutils.widgets.text("p_enviroment", "")
v_enviroment = dbutils.widgets.get("p_enviroment")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2024-12-16")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Paso 1 Leer el archivo JSON usando "DataframeReader" de Spark

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/commont_functions"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# COMMAND ----------

movies_casts_schema = StructType(
fields=[
StructField("movieId", IntegerType(), True),
StructField("personId", IntegerType(), True),
StructField("characterName", StringType(), True),
StructField("genderId", IntegerType(), True),
StructField("castOrder", IntegerType(), True)

])

# COMMAND ----------

movies_casts_df = spark.read \
.schema(movies_casts_schema)\
.option("multiline", True)\
.json(f"{bronze_folder_path}/{v_file_date}/movie_cast.json")


# COMMAND ----------

# MAGIC %md
# MAGIC #### Paso 2 - Renombrar las columnas y añadir nuevas columnas
# MAGIC 1. "movieID" renombrar a "movie_id"
# MAGIC 2. Agregar columnas "ingestion_date" y "enviroment"
# MAGIC 3. "characterName" renombrar a "character_name"
# MAGIC 4. Agregar las columnas "ingestion_date" y "enviroment"

# COMMAND ----------

from pyspark.sql.functions import col, concat, current_timestamp, lit

# COMMAND ----------

movies_casts_with_columns_df =add_ingestion_date(movies_casts_df)\
                  .withColumn("enviroment", lit(v_enviroment))\
                    .withColumn("file_date", lit(v_file_date))\
                .withColumnRenamed("movieId", "movie_id") \
                .withColumnRenamed("personId", "person_id") \
                .withColumnRenamed("characterName", "character_name")
       

# COMMAND ----------

display(movies_casts_with_columns_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Paso 3 - Eliminar columnas no deseadas
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

movies_casts_final_df = movies_casts_with_columns_df.drop("genderId", "castOrder")


# COMMAND ----------

# MAGIC %md
# MAGIC #### Paso 4 - Escribir la salida en formato "Parquet"

# COMMAND ----------

merge_condition = 'tgt.movie_id = src.movie_id AND tgt.person_id =src.person_id AND tgt.file_date = src.file_date'
merge_delta_lake(movies_casts_final_df, "movie_silver", "movies_casts", silver_folder_path, merge_condition, "file_date")


# COMMAND ----------

# MAGIC %sql
# MAGIC select file_date, count(1)
# MAGIC from movie_silver.movies_casts
# MAGIC group by file_date 

# COMMAND ----------

dbutils.notebook.exit("success")
