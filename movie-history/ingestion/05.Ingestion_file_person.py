# Databricks notebook source
# MAGIC %md
# MAGIC #### Ingesta del archivo "person.json"

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

name_schema = StructType(
fields=[
StructField("forename", StringType(), True),
StructField("surname", StringType(), True),
])
                         

# COMMAND ----------

persons_schema = StructType(
fields=[
StructField("personId", IntegerType(), True),
StructField("personName", name_schema)
])

# COMMAND ----------

persons_df = spark.read \
.schema(persons_schema)\
.json(f"{bronze_folder_path}/{v_file_date}/person.json")


# COMMAND ----------

display(persons_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Paso 2 - Renombrar las columnas y añadir nuevas columnas
# MAGIC 1. "personId" renombrar a "person_id"
# MAGIC 2. Agregar columnas "ingestion_date" y "enviroment"
# MAGIC 3. Agregar la columna "name" a partir de la concatenacion de "forename" y "surename"

# COMMAND ----------

from pyspark.sql.functions import col, concat, current_timestamp, lit

# COMMAND ----------


 persons_with_columns_df = add_ingestion_date(persons_df)\
                .withColumnRenamed("personId", "person_id") \
                .withColumn("enviroment", lit(v_enviroment))\
                .withColumn("file_date", lit(v_file_date))\
                .withColumn("name",
                          concat(col("personName.forename"),
                                lit(" "),
                                col("personName.surname"))
                ).select("person_id", "name", "ingestion_date", "enviroment", "file_date")      



# COMMAND ----------

display(persons_with_columns_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Paso 3 -Eliminar las columnas "requeridas"

# COMMAND ----------

persons_final_df = persons_with_columns_df.drop(col("personName"))


# COMMAND ----------

display(persons_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Paso 4 - Escribir la salida en formato parquet

# COMMAND ----------

merge_condition = 'tgt.person_id = src.person_id AND tgt.file_date = src.file_date'
merge_delta_lake(persons_final_df, "movie_silver", "persons", silver_folder_path, merge_condition, "file_date")


# COMMAND ----------

# MAGIC %sql
# MAGIC select file_date, count(1)
# MAGIC from movie_silver.persons
# MAGIC group by file_date 

# COMMAND ----------

dbutils.notebook.exit("success")
