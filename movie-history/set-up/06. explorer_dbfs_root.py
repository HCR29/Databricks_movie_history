# Databricks notebook source
# MAGIC %md
# MAGIC ### Explorer DBFS root
# MAGIC 1. Montar los directorios en el almacenamiento
# MAGIC 2. Mostrar el contenido de un directorio DBFS dentro del root
# MAGIC
# MAGIC   - Con DBFS
# MAGIC   - Sin DBFS
# MAGIC 3. Mostrar el conteindo del sistema de archivos local
# MAGIC 4. Interactuar con el explorardor de archivos DBFS
# MAGIC 5. Cargar archivo en el filestore 

# COMMAND ----------

# MAGIC %fs
# MAGIC ls dbfs:/

# COMMAND ----------

dbutils.fs.ls("/")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls dbfs:/databricks-datasets

# COMMAND ----------

dbutils.fs.ls("/databricks-datasets")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls file:/

# COMMAND ----------

display(dbutils.fs.ls("dbfs:/FileStore/"))


# COMMAND ----------

# MAGIC %fs
# MAGIC ls dbfs:/
