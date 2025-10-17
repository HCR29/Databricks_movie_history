# Databricks notebook source
# MAGIC %md
# MAGIC #### Mount (Montar) Azure Data Lake para el proyecto

# COMMAND ----------

def mount_adls(storage_account_name, container_name):
    #Obtener Secret Key de key vault
    client_id = dbutils.secrets.get(scope="movie-history-secret-scope", key="client-id")
    tenant_id = dbutils.secrets.get(scope="movie-history-secret-scope", key="tenant-id")
    client_secret = dbutils.secrets.get(scope="movie-history-secret-scope", key="client-secret")

    #Establecer confiuraciones de spark 
    configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}
    
    # Desmontar el montaje (unmount) el montaje (mount) si ya existe
    if any (mount.mountPoint == f"/mnt/{storage_account_name}/{container_name}" for mount in dbutils.fs.mounts()):
          dbutils.fs.unmount(f"/mnt/{storage_account_name}/{container_name}")
          
    # Mount (Montar) el Contenedor del Storage Account
    dbutils.fs.mount(
    source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
    mount_point = f"/mnt/{storage_account_name}/{container_name}",
    extra_configs = configs)

    #Listar los Mount
    display(dbutils.fs.mounts())    

# COMMAND ----------

# MAGIC %md
# MAGIC #### Montar (Mount) el contenedor "bronze"

# COMMAND ----------

mount_adls("moviehistory07", "bronze")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Montar (Mount) el contenedor "silver"

# COMMAND ----------

mount_adls("moviehistory07", "silver")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Montar (Mount) el contenedor "gold"

# COMMAND ----------

mount_adls("moviehistory07", "gold")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Montar (Mount) el contenedor "demo"

# COMMAND ----------

mount_adls("moviehistory07", "demo")
