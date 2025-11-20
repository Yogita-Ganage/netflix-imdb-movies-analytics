# Databricks notebook source
# MAGIC %md
# MAGIC #Bronze Layer – Ingestion (Assumptions & Design Notes)
# MAGIC **Purpose:**  
# MAGIC The Bronze layer stores raw data exactly as received, with no business rules applied.  
# MAGIC All datasets are ingested from the workspace Volume, converted to Delta format, and enriched with basic metadata for lineage.
# MAGIC
# MAGIC **Key Assumptions**
# MAGIC - IMDB data is available year-wise in separate folders (e.g. /imdb/2010/).  
# MAGIC - Only IMDB files from 2010–2025 are required, based on the task specification.  
# MAGIC   Earlier years are ignored intentionally.
# MAGIC - Column names are sanitized to be Unity Catalog-safe (no spaces, special characters).
# MAGIC - Metadata added in Bronze includes:
# MAGIC   - ingestionDate
# MAGIC   - source
# MAGIC   - fileName
# MAGIC   - fileYear (for IMDB datasets only)
# MAGIC
# MAGIC **Output**
# MAGIC Each Bronze table serves as the raw foundation for Silver transformations.
# MAGIC Bronze produces five tables:
# MAGIC - netflix_titles_raw
# MAGIC - netflix_credits_raw
# MAGIC - imdb_movies_raw
# MAGIC - imdb_advanced_movies_details_raw
# MAGIC - imdb_merged_movies_raw

# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC ##Catalog and Schema Setup

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG workspace;
# MAGIC CREATE SCHEMA IF NOT EXISTS bronze

# COMMAND ----------

# MAGIC %md
# MAGIC ##Imports

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql import DataFrame
import re

# COMMAND ----------

# MAGIC %md
# MAGIC ##Helper Functions

# COMMAND ----------


# consistent timestamps
spark.conf.set("spark.sql.session.timeZone", "UTC")

# Raw Paths (Volumes)

BASE_RAW_PATH    = "/Volumes/workspace/landing/inbox/source_v1"
NETFLIX_RAW_PATH = f"{BASE_RAW_PATH}/netflix"
IMDB_RAW_PATH    = f"{BASE_RAW_PATH}/imdb"

# Netflix CSV paths
NETFLIX_TITLES_PATH  = f"{NETFLIX_RAW_PATH}/titles.csv"
NETFLIX_CREDITS_PATH = f"{NETFLIX_RAW_PATH}/credits.csv"

# IMDB years as per acceptance criteria (2010–2025)
IMDB_YEARS = list(range(2010, 2026))



# Helper: clean column names for Unity Catalog

def clean_column_names(df: DataFrame) -> DataFrame:
    """
    Make column names Unity Catalog–safe:
    - strip spaces + hidden chars
    - replace ANY non [A-Za-z0-9_] with '_'
    - collapse multiple '_' into one
    - if name starts with digit, prefix with 'c_'
    """

    new_cols = []
    for col in df.columns:
        col_clean = col.strip()
        col_clean = re.sub(r"[\t\n\r]", "", col_clean)        
        col_clean = re.sub(r"[^A-Za-z0-9_]", "_", col_clean)  
        col_clean = re.sub(r"_+", "_", col_clean)            
        if re.match(r"^[0-9]", col_clean):                    
            col_clean = "c_" + col_clean
        if col_clean == "":
            col_clean = "col_unnamed"
        new_cols.append(col_clean)

    for old, new in zip(df.columns, new_cols):
        if old != new:
            df = df.withColumnRenamed(old, new)

    return df


# Generic helper for Netflix Bronze tables

def create_bronze_table(
    csv_path: str,
    table_name: str,
    source_name: str
) -> None:
    """
    Read a raw CSV and create a Bronze Delta table in workspace.bronze.

    - Keeps data as-is (no business transforms)
    - Cleans column names for UC
    - Adds ingestion metadata columns
    """

    raw_df: DataFrame = (
        spark.read
             .option("header", "true")
             .option("quote", "\"")       
             .option("escape", "\"")
             .option("multiLine", "true") 
             .option("mode", "PERMISSIVE")
             .csv(csv_path)
    )

    raw_df = clean_column_names(raw_df)

    bronze_df = (
        raw_df
        .withColumn("ingestionDate", F.current_timestamp())
        .withColumn("source", F.lit(source_name))
        .withColumn("fileName", F.lit(csv_path))  
    )

    # For small–medium CSVs: avoid many tiny files
    bronze_df = bronze_df.repartition(1)

    (
        bronze_df.write
                 .format("delta")
                 .mode("overwrite")
                 .option("overwriteSchema", "true")
                 .saveAsTable(f"workspace.bronze.{table_name}")
    )

    print(f"Created Bronze table workspace.bronze.{table_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ##NETFLIX + IMDB Bronze tables

# COMMAND ----------

# Netflix Bronze tables

create_bronze_table(
    csv_path=NETFLIX_TITLES_PATH,
    table_name="netflix_titles_raw",
    source_name="netflix_titles_csv"
)

create_bronze_table(
    csv_path=NETFLIX_CREDITS_PATH,
    table_name="netflix_credits_raw",
    source_name="netflix_credits_csv"
)


# IMDB Bronze helper: loop over years

def build_imdb_bronze_table(
    file_name_pattern: str,
    table_name: str,
    source_name: str
) -> None:
    """
    Read IMDB CSVs per year (2010–2025), clean column names,
    add metadata, union them and write to a single Bronze Delta table.

    file_name_pattern examples (relative to each year folder):
        "imdb_movies_{year}.csv"
        "advanced_movies_details_{year}.csv"
        "merged_movies_data_{year}.csv"
    """

    combined_df = None

    for year in IMDB_YEARS:
        path = f"{IMDB_RAW_PATH}/{year}/" + file_name_pattern.format(year=year)
        print(f"Reading: {path}")

        df_year = (
            spark.read
                 .option("header", "true")
                 .option("quote", "\"")
                 .option("escape", "\"")
                 .option("multiLine", "true")
                 .option("mode", "PERMISSIVE")
                 .csv(path)
        )

        df_year = clean_column_names(df_year)

        df_year = (
            df_year
            .withColumn("fileYear", F.lit(year))
            .withColumn("ingestionDate", F.current_timestamp())
            .withColumn("source", F.lit(source_name))
            .withColumn("fileName", F.lit(path)) 
        )

        combined_df = (
            df_year
            if combined_df is None
            else combined_df.unionByName(df_year, allowMissingColumns=True)
        )

    # Reduce small files
    combined_df = combined_df.repartition(4)

    (
        combined_df.write
                   .format("delta")
                   .mode("overwrite")
                   .option("overwriteSchema", "true")
                   .saveAsTable(f"workspace.bronze.{table_name}")
    )

    print(f"Created Bronze table workspace.bronze.{table_name}")


# IMDB Bronze tables (3 unified tables)

build_imdb_bronze_table(
    file_name_pattern="imdb_movies_{year}.csv",
    table_name="imdb_movies_raw",
    source_name="imdb_movies_csv"
)

build_imdb_bronze_table(
    file_name_pattern="advanced_movies_details_{year}.csv",
    table_name="imdb_advanced_movies_details_raw",
    source_name="imdb_advanced_movies_details_csv"
)

build_imdb_bronze_table(
    file_name_pattern="merged_movies_data_{year}.csv",
    table_name="imdb_merged_movies_raw",
    source_name="imdb_merged_movies_data_csv"
)


# COMMAND ----------

# MAGIC %md
# MAGIC #Verification

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG workspace;
# MAGIC
# MAGIC SELECT 'netflix_titles_raw' AS table_name, COUNT(*) AS row_count
# MAGIC FROM bronze.netflix_titles_raw
# MAGIC UNION ALL
# MAGIC SELECT 'netflix_credits_raw', COUNT(*) FROM bronze.netflix_credits_raw
# MAGIC UNION ALL
# MAGIC SELECT 'imdb_movies_raw', COUNT(*) FROM bronze.imdb_movies_raw
# MAGIC UNION ALL
# MAGIC SELECT 'imdb_advanced_movies_details_raw', COUNT(*) FROM bronze.imdb_advanced_movies_details_raw
# MAGIC UNION ALL
# MAGIC SELECT 'imdb_merged_movies_raw', COUNT(*) FROM bronze.imdb_merged_movies_raw;
# MAGIC

# COMMAND ----------


# Netflix Row Count Validation


# RAW row counts (direct CSV read, same options as Bronze)
raw_titles_count = (
    spark.read
         .option("header", True)
         .option("quote", "\"")
         .option("escape", "\"")
         .option("multiLine", True)
         .option("mode", "PERMISSIVE")
         .csv(f"{NETFLIX_RAW_PATH}/titles.csv")
         .count()
)

raw_credits_count = (
    spark.read
         .option("header", True)
         .option("quote", "\"")
         .option("escape", "\"")
         .option("multiLine", True)
         .option("mode", "PERMISSIVE")
         .csv(f"{NETFLIX_RAW_PATH}/credits.csv")
         .count()
)

# BRONZE row counts (Delta tables)
bronze_titles_count  = spark.table("workspace.bronze.netflix_titles_raw").count()
bronze_credits_count = spark.table("workspace.bronze.netflix_credits_raw").count()





# COMMAND ----------


# Print summary


print("RAW Titles rows:     ", raw_titles_count)
print("BRONZE Titles rows:  ", bronze_titles_count)
print("Titles removed:      ", raw_titles_count - bronze_titles_count)

print("\nRAW Credits rows:    ", raw_credits_count)
print("BRONZE Credits rows: ", bronze_credits_count)
print("Credits removed:     ", raw_credits_count - bronze_credits_count)
