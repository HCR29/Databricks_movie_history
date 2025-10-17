# Databricks notebook source
# MAGIC %md
# MAGIC #### Acceder a Azure Data Lake Storage mediante Token SAS
# MAGIC
# MAGIC 1. Establecer la configuracion de spark "SAS token"
# MAGIC 2. Listar archivos del contenedor "demo"
# MAGIC 3. Leer datos del archivo "movie.csv"

# COMMAND ----------

dbutils.secrets.get(scope = "movie-history-secret-scope", key = "movie-sas-token")

# COMMAND ----------

movie_sas_token = dbutils.secrets.get(scope = "movie-history-secret-scope", key = "movie-sas-token")

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.moviehistory07.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.moviehistory07.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.moviehistory07.dfs.core.windows.net", movie_sas_token)

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@moviehistory07.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@moviehistory07.dfs.core.windows.net/movie.csv"))
