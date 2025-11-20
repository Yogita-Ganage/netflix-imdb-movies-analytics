# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer – Analytics Model (Assumptions & Purpose)
# MAGIC
# MAGIC **Purpose:**  
# MAGIC Provide a simplified, analytics friendly version of the unified movies dataset,  
# MAGIC and support the required analytical queries without reprocessing logic.
# MAGIC
# MAGIC **Key Assumptions**
# MAGIC - Analytical fields such as:
# MAGIC   - 'decade'
# MAGIC   - 'runtimeBucket'  
# MAGIC   are added to support trend and distribution analysis.
# MAGIC - Only data coming through the Silver unified model (silver.movies) is used.
# MAGIC
# MAGIC **Analytical Queries Supported by Gold**
# MAGIC Gold must enable the following analyses (implemented below as SQL cells):
# MAGIC   - 1. Number of titles released per year
# MAGIC   - 2. Top directors by content production  
# MAGIC   - 3. Most common age certifications
# MAGIC
# MAGIC ## Output
# MAGIC - 'gold.movies' – the single fact table used to drive all required analytics.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ##Catalog and Schema Setup

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG workspace;
# MAGIC CREATE SCHEMA IF NOT EXISTS gold

# COMMAND ----------

# MAGIC %md
# MAGIC ##Imports

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql import DataFrame

# COMMAND ----------

# MAGIC %md
# MAGIC #Helper Functions

# COMMAND ----------

# --- Helpers (same camelCase as Silver) ---

def to_camel_case(col_name: str) -> str:
    if col_name is None:
        return col_name
    raw = col_name.strip()
    if raw == "":
        return raw
    if "_" not in raw and " " not in raw and not raw.isupper():
        return raw[0].lower() + raw[1:]
    parts = raw.replace(" ", "_").split("_")
    parts = [p for p in parts if p]
    if not parts:
        return col_name
    first = parts[0].lower()
    rest = [p[:1].upper() + p[1:].lower() for p in parts[1:]]
    return first + "".join(rest)

def rename_columns_to_camel(df: DataFrame) -> DataFrame:
    for c in df.columns:
        new_name = to_camel_case(c)
        if new_name != c:
            df = df.withColumnRenamed(c, new_name)
    return df



# COMMAND ----------

# MAGIC %md
# MAGIC #Gold table

# COMMAND ----------


#  1. Build Gold movies table 

silver_movies = spark.table("workspace.silver.movies")

gold_movies = (
    silver_movies
    # helper dimension: decade for trend analysis
    .withColumn(
        "decade",
        (F.col("releaseYear") / 10).cast("int") * 10
    )
    # helper dimension: runtime bucket for distribution analysis
    .withColumn(
        "runtimeBucket",
        F.when(F.col("runtimeMinutes").isNull(), F.lit("Unknown"))
         .when(F.col("runtimeMinutes") < 60, "Under 1h")
         .when(F.col("runtimeMinutes") < 90, "60–89 min")
         .when(F.col("runtimeMinutes") < 120, "90–119 min")
         .otherwise("120+ min")
    )
)

gold_movies = rename_columns_to_camel(gold_movies)

gold_movies.write.mode("overwrite").format("delta").saveAsTable(
    "workspace.gold.movies"
)
