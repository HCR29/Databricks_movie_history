# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ### Paso 1 Leer el archivo csv usando "DataframeReader" de Spark

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

from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# COMMAND ----------

genre_schema = StructType(fields=[
  StructField("genreID", IntegerType(), False),
  StructField("genreName", StringType(), True)])

# COMMAND ----------

genre_df = spark.read \
                .option("header", True) \
                .schema(genre_schema) \
                .csv(f"{bronze_folder_path}/{v_file_date}/genre.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Paso 2 - Seleccionar solo las columnas "requeridas" 

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

genre_selected_df = genre_df.select(col("genreID"), col("genreName"))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Paso 3 - Cambiar el nombre de las columnas segun lo "requerido"

# COMMAND ----------

genre_renamed_df = genre_selected_df.withColumnRenamed("genreID", "genre_id").withColumnRenamed("genreName", "genre_name")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Paso 4 - Agregar la columna "ingestion_date" y "enviroment" al DataFrame

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

genre_final_df = add_ingestion_date(genre_renamed_df)\
                  .withColumn("enviroment", lit(v_enviroment))\
                  .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Paso 5 - Escribir datos en el datalake en formato "Parquet"
# MAGIC

# COMMAND ----------

## para tener la hora de Perú
spark.conf.set("spark.sql.session.timeZone", "America/Lima")

# COMMAND ----------

genre_final_df.write.mode("overwrite").format("delta").saveAsTable("movie_silver.genres")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from movie_silver.genres

# COMMAND ----------

dbutils.notebook.exit("success")
