# Databricks notebook source
# MAGIC %md
# MAGIC #### Acceder a Azure Data Lake Storage mediante Service Principal
# MAGIC
# MAGIC 1. Registrar la Aplicacion en Azure Entra ID/Service Principal
# MAGIC 2. Generar un secreto (Contraseña) para la aplicacion
# MAGIC 3. Configurar el Role "Storage con APP / Client Id, Directory/ Tenand Id & Secret
# MAGIC 4. Asignar el Role "Storage Blob Data Contributor" al Data Lake

# COMMAND ----------

client_id = "cd71f8d4-01bf-412a-a3f9-928444652680"
tenant_id = "131ae39e-1e5e-4b74-a7c0-57f98f0f0866"
client_secret = "MsM8Q~pvu_j-.aeb_ZzaXRI2r2fWIWK1qvZmZalZ"


# COMMAND ----------


spark.conf.set("fs.azure.account.auth.type.moviehistory07.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.moviehistory07.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.moviehistory07.dfs.core.windows.net", client_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.moviehistory07.dfs.core.windows.net", client_secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.moviehistory07.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@moviehistory07.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@moviehistory07.dfs.core.windows.net/movie.csv"))

# COMMAND ----------

##VIDEO 43 DE UDEMY ABAJO

# COMMAND ----------

# DBTITLE 1,ena
client_id = dbutils.secrets.get(scope="movie-history-secret-scope", key="client-id")
tenant_id = dbutils.secrets.get(scope="movie-history-secret-scope", key="tenant-id")
client_secret = dbutils.secrets.get(scope="movie-history-secret-scope", key="client-secret")


# COMMAND ----------


spark.conf.set("fs.azure.account.auth.type.moviehistory07.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.moviehistory07.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.moviehistory07.dfs.core.windows.net", client_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.moviehistory07.dfs.core.windows.net", client_secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.moviehistory07.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@moviehistory07.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@moviehistory07.dfs.core.windows.net/movie.csv"))
