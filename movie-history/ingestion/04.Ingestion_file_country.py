# Databricks notebook source
# MAGIC %md
# MAGIC #### Ingesta del archivo "country.json"

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

# MAGIC %md
# MAGIC
# MAGIC ### Paso 1 Leer el archivo JSON usando "DataframeReader" de Spark

# COMMAND ----------

countries_schema = "countryId INT, countryIsoCode STRING, countryName STRING"

# COMMAND ----------

countries_df = spark.read.schema(countries_schema) \
            .schema(countries_schema)\
            .json(f"{bronze_folder_path}/{v_file_date}/country.json")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Paso 2 - Eliminar las columnas no deseadas del Dataframe

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

countries_df = countries_df.select(col("countryId"), col("countryName"))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Paso 3 - Cambiar el nombre de las columnas segun lo "requerido"

# COMMAND ----------

country_renamed_df = countries_df.withColumnRenamed("countryId", "country_id").withColumnRenamed("countryName", "country_name")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Paso 4 - Agregar la columna "ingestion_date" y "enviroment" al DataFrame

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

country_select_df = add_ingestion_date(country_renamed_df)\
                  .withColumn("enviroment", lit(v_enviroment))\
                  .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Paso 5 - Escribir datos en el datalake en formato "Parquet"
# MAGIC

# COMMAND ----------

country_select_df.write.mode("overwrite").format("delta").saveAsTable("movie_silver.countries")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from movie_silver.countries

# COMMAND ----------

dbutils.notebook.exit("success")
