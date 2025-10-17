# Databricks notebook source
# MAGIC %md
# MAGIC #### Ingesta del archivo "language_role.json"

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

name_schema = StructType(
fields=[
StructField("roleId", IntegerType(), False),
StructField("languageRole", StringType(), True)
])
                         

# COMMAND ----------

language_role_df = spark.read \
.schema(name_schema)\
.option("multiline", "true")\
.json(f"{bronze_folder_path}/{v_file_date}/language_role.json")


# COMMAND ----------

display(language_role_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Paso 2 - Renombrar las columnas y añadir nuevas columnas
# MAGIC 1. "roleId" renombrar a "role_id" , "languageRole" a "language_role"
# MAGIC 2. Agregar columnas "ingestion_date" y "enviroment"
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import col, concat, current_timestamp, lit

# COMMAND ----------

language_role_with_columns_df = add_ingestion_date(language_role_df)\
                                .withColumn("enviroment", lit(v_enviroment))\
                                .withColumn("file_date", lit(v_file_date))\
                                .withColumnRenamed("roleId", "role_id")\
                                .withColumnRenamed("languageRole", "language_role")


# COMMAND ----------

# MAGIC %md
# MAGIC #### Paso 3 - Escribir la salida en formato parquet

# COMMAND ----------

language_role_with_columns_df.write.mode("overwrite").format("delta").saveAsTable("movie_silver.languages_roles")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from movie_silver.languages_roles

# COMMAND ----------

dbutils.notebook.exit("success")
