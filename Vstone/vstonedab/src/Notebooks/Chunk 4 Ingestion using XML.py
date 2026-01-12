# Databricks notebook source
# MAGIC %md
# MAGIC ##1. Modular Ingestion Logic

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC This section replaces multiple repetitive code blocks with a single, dynamic script.
# MAGIC
# MAGIC ####Logic: 
# MAGIC
# MAGIC The script uses dbutils.widgets to accept external parameters such as file paths and table names. It then applies a standard transformation schema, including audit columns like load_dt (timestamp) and source (origin tag). 
# MAGIC
# MAGIC ####Why this code?
# MAGIC
# MAGIC Hard-coding paths for every table is difficult to maintain. By using widgets, this single notebook can be reused across multiple Databricks Job tasks, significantly reducing code redundancy and making the pipeline scalable for new XML sources.

# COMMAND ----------

# notebooks/xml_generic_ingestion.py
from pyspark.sql.functions import current_timestamp, lit

# 1. Define the 'landing spots' for Job Parameters
dbutils.widgets.text("source_path", "")
dbutils.widgets.text("target_table", "")
dbutils.widgets.text("source_tag", "")

# 2. Retrieve values pushed from the DAB Job Task
source_path = dbutils.widgets.get("source_path")
target_table = dbutils.widgets.get("target_table")
source_tag = dbutils.widgets.get("source_tag")

# 3. Logic: Process any XML source into any target table
if source_path and target_table:
    print(f"Ingesting {source_tag} into {target_table}...")
    
    df = (spark.read
          .format("xml")
          .option("rowTag", "item")
          .load(source_path)
          .withColumn("load_dt", current_timestamp())
          .withColumn("source", lit(source_tag)))
    
    # Save to the specific table requested by the Job Task
    df.write.mode("append").format("delta").saveAsTable(target_table)
else:
    raise ValueError("Missing required parameters: source_path or target_table")

# COMMAND ----------


