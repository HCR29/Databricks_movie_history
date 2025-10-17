# Databricks notebook source
# MAGIC %md
# MAGIC #### Ingesta del archivo "movie_genre.json"

# COMMAND ----------

dbutils.widgets.text("p_enviroment", "")
v_enviroment = dbutils.widgets.get("p_enviroment")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2024-12-30")
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

movie_genre_schema = StructType(
fields=[
StructField("movieId", IntegerType(), True),
StructField("genreId", IntegerType(), True)
])

# COMMAND ----------

movie_genre_df = spark.read \
.schema(movie_genre_schema)\
.json(f"{bronze_folder_path}/{v_file_date}/movie_genre.json")


# COMMAND ----------

display(movie_genre_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Paso 2 - Renombrar las columnas y añadir nuevas columnas
# MAGIC 1. "movieID" renombrar a "movie_id"
# MAGIC 2. Agregar columnas "ingestion_date" y "enviroment"

# COMMAND ----------

from pyspark.sql.functions import col, concat, current_timestamp, lit

# COMMAND ----------

genre_movie_columns_df =add_ingestion_date(movie_genre_df)\
                  .withColumn("enviroment", lit(v_enviroment))\
                    .withColumn("file_date", lit(v_file_date))\
                .withColumnRenamed("movieId", "movie_id") \
                .withColumnRenamed("genreId", "genre_id")

# COMMAND ----------

display(genre_movie_columns_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Paso 4 - Escribir la salida en formato parquet
# MAGIC 1. Particionar en movie_id

# COMMAND ----------

#overwrite_partition(genre_movie_columns_df, "movie_silver", "movies", "file_date")

# COMMAND ----------



# COMMAND ----------

merge_condition = 'tgt.movie_id = src.movie_id AND tgt.genre_id =src.genre_id AND tgt.file_date = src.file_date'
merge_delta_lake(genre_movie_columns_df, "movie_silver", "movies_genres", silver_folder_path, merge_condition, "file_date")




# COMMAND ----------

# MAGIC %sql
# MAGIC select file_date, count(1)
# MAGIC from movie_silver.movies_genres
# MAGIC group by file_date 

# COMMAND ----------

dbutils.notebook.exit("success")
