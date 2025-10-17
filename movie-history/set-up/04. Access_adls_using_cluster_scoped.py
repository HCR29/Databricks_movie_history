# Databricks notebook source
# MAGIC %md
# MAGIC #### Acceder a Azure Data Lake Storage mediante Ambito de cluster
# MAGIC #### paso a seguir
# MAGIC
# MAGIC 1. Establecer configuracion spark "fs.azure.acount.key" en el cluster
# MAGIC 2. Listar archivo del contenedor "demo"
# MAGIC 3. Leer datos del archivo "movie.csv"

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@moviehistory07.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@moviehistory07.dfs.core.windows.net/movie.csv"))
