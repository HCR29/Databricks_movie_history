# Databricks notebook source
# MAGIC %md
# MAGIC #### Acceder a Azure Data Lake Storage mediante Access Key
# MAGIC
# MAGIC 1. Establecer la configuracion de spark "Access token"
# MAGIC 2. Listar archivos del contenedor "demo"
# MAGIC 3. Leer datos del archivo "movie.csv"

# COMMAND ----------

print(3-2)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Los 2 notebook de abajo son del 06.

# COMMAND ----------

movie_access_key = dbutils.secrets.get(scope = "movie-history-secret-scope", key = "movie-access-key")

# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.moviehistory07.dfs.core.windows.net", movie_access_key
    )

# COMMAND ----------

### De aqui abajo es el 1


# COMMAND ----------

dbutils.fs.ls("abfss://demo@moviehistory07.dfs.core.windows.net")

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@moviehistory07.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@moviehistory07.dfs.core.windows.net/movie.csv"))

# COMMAND ----------

movie_access_key = dbutils.secrets.get(scope = "movie-history-secret-scope", key = "movie-access-key")

# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.moviehistory007.dfs.core.windows.net",
    movie_access_key
    )

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@moviehistory007.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@moviehistory007.dfs.core.windows.net/movie.csv"))
