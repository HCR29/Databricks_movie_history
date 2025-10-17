# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ### Paso 1 Leer el archivo csv usando "DataframeReader" de Spark

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

from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# COMMAND ----------

languages_schema = StructType([
  StructField("languageID", IntegerType(), False),
  StructField("languageCode", StringType(), True),
  StructField("languageName", StringType(), True)])

# COMMAND ----------

languages_df = spark.read \
                .option("header", True) \
                .schema(languages_schema) \
                .csv(f"{bronze_folder_path}/{v_file_date}/language.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Paso 2 - Seleccionar solo las columnas "requeridas" 

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

languages_selected_df = languages_df.select(col("languageID"), col("languageName"))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Paso 3 - Cambiar el nombre de las columnas segun lo "requerido"

# COMMAND ----------

languages_renamed_df = languages_selected_df.withColumnRenamed("languageId", "language_id").withColumnRenamed("languageName", "language_name")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Paso 4 - Agregar la columna "ingestion_date" y "enviroment" al DataFrame

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

languages_final_df = add_ingestion_date(languages_renamed_df)\
                  .withColumn("enviroment", lit(v_enviroment))\
                  .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Paso 5 - Escribir datos en el datalake en formato "Parquet"
# MAGIC

# COMMAND ----------

languages_final_df.write.mode("overwrite").format("delta").saveAsTable("movie_silver.languages")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from movie_silver.languages

# COMMAND ----------

dbutils.notebook.exit("success")
