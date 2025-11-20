# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer – Transformation & Conformance (Assumptions & Design Notes)
# MAGIC
# MAGIC **Purpose:**  
# MAGIC Transform raw Bronze data into clean, standardized, analytics-ready tables while applying all business rules defined in the task.
# MAGIC
# MAGIC ## Key Assumptions
# MAGIC ### General Processing
# MAGIC - All column names must be converted to **lowerCamelCase**.
# MAGIC - Text cleanup includes trimming whitespace and converting empty strings to `NULL`.
# MAGIC - Titles such as "1. The Matrix" are cleaned by removing leading numeric prefixes.
# MAGIC - Titles that are NULL, empty, or "[]" are considered junk and removed.
# MAGIC
# MAGIC ### Netflix Processing
# MAGIC - Only records where `type == "MOVIE" are required for the analytical model.
# MAGIC - If multiple Netflix movies have the same (title, releaseYear),  
# MAGIC - the version with the highest imdbScore is retained.
# MAGIC - Director assignment:
# MAGIC   - Taken from 'netflix_credits_raw'.
# MAGIC   - If multiple directors exist, the earliest person_id is selected.
# MAGIC - Runtime, IMDB score, TMDB metrics, votes, and release year are cast to numeric types.
# MAGIC
# MAGIC ### IMDB Processing
# MAGIC - Only 'imdb_merged_movies_raw' is required for matching with Netflix movies.
# MAGIC - Release year is derived from:
# MAGIC   1. 'release_date' (parsed), else  
# MAGIC   2. Fallback to raw 'year' column.
# MAGIC - Runtime is normalized to integer minutes from formats like "1h 42m" / "95m".
# MAGIC - 'imdbRating' and 'imdbVotes' are parsed as numeric fields.
# MAGIC - If multiple entries exist for the same (titleNorm, releaseYear),  
# MAGIC   the row with highest IMDB rating, then highest votes is selected.
# MAGIC
# MAGIC ### Joining Netflix & IMDB
# MAGIC - Join priority:
# MAGIC   1. Primary: imdbId match  
# MAGIC   2. Fallback: (titleNorm, releaseYear)
# MAGIC - IMDB rating & votes priority:
# MAGIC   - Use **IMDB dataset first**
# MAGIC   - Fallback to Netflix 'imdbScore' and 'imdbVotes' when IMDB data is missing.
# MAGIC - Only movies coming from Netflix should appear in the final model  
# MAGIC   (Silver movies table is left-joined from Netflix → IMDB).
# MAGIC
# MAGIC ## Output Silver Tables
# MAGIC - silver.netflix_movies – cleaned Netflix dataset with one movie per title/year.
# MAGIC - silver.imdb_movies – deduplicated IMDB dataset with standardized fields.
# MAGIC - silver.movies – unified movie model containing:
# MAGIC   - title, releaseYear, runtimeMinutes
# MAGIC   - director, genres, productionCountries
# MAGIC   - imdbRating (IMDB-first), imdbVotes (IMDB-first)
# MAGIC   - tmdbPopularity, tmdbScore
# MAGIC   - hasImdbMatch flag

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ##Catalog and Schema Setup

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG workspace;
# MAGIC CREATE SCHEMA IF NOT EXISTS silver

# COMMAND ----------

# MAGIC %md
# MAGIC ##Imports

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql import DataFrame

# COMMAND ----------

# MAGIC %md
# MAGIC ##Helper Functions

# COMMAND ----------

def clean_string_columns(df: DataFrame) -> DataFrame:
    """
    Trim whitespace and convert empty strings to NULL for all string columns.
    """
    for col_name, col_type in df.dtypes:
        if col_type == "string":
            df = df.withColumn(
                col_name,
                F.when(F.trim(F.col(col_name)) == "", F.lit(None))
                 .otherwise(F.trim(F.col(col_name)))
            )
    return df


def to_camel_case(col_name: str) -> str:
    """
    Convert snake_case / spaces / UPPER to lowerCamelCase.
    If the name already looks like camelCase or PascalCase, keep it
    (just force first letter to lower-case).
    """
    if col_name is None:
        return col_name

    raw = col_name.strip()
    if raw == "":
        return raw

    # Already a single token (no _ / space) and not all upper → treat as camel/pascal
    if "_" not in raw and " " not in raw and not raw.isupper():
        return raw[0].lower() + raw[1:]

    # Otherwise, snake/space → camel
    parts = raw.replace(" ", "_").split("_")
    parts = [p for p in parts if p]
    if not parts:
        return col_name

    first = parts[0].lower()
    rest = [p[:1].upper() + p[1:].lower() for p in parts[1:]]
    return first + "".join(rest)


def rename_columns_to_camel(df: DataFrame) -> DataFrame:
    """
    Return a new DataFrame with all columns renamed to camelCase.
    """
    for c in df.columns:
        new_name = to_camel_case(c)
        if new_name != c:
            df = df.withColumnRenamed(c, new_name)
    return df


def normalize_age_cert(df: DataFrame) -> DataFrame:
    """
    Standardise ageCertification to a consistent format:
    - Uppercase, no spaces
    - Replace underscores with dashes
    - Map common variants like PG13 -> PG-13
    - Fill nulls with 'NR'
    """
    if "ageCertification" not in df.columns:
        return df

    col = F.col("ageCertification")

    df = df.withColumn("ageCertification", F.upper(F.trim(col)))
    df = df.withColumn("ageCertification", F.regexp_replace("ageCertification", r"\s+", ""))
    df = df.withColumn("ageCertification", F.regexp_replace("ageCertification", "_", "-"))

    df = (
        df
        .withColumn(
            "ageCertification",
            F.when(F.col("ageCertification") == "PG13", "PG-13")
             .when(F.col("ageCertification") == "TVMA", "TV-MA")
             .when(F.col("ageCertification") == "NC17", "NC-17")
             .otherwise(F.col("ageCertification"))
        )
        .withColumn(
            "ageCertification",
            F.when(
                F.col("ageCertification").isNull() | (F.col("ageCertification") == ""),
                F.lit("NR")
            ).otherwise(F.col("ageCertification"))
        )
    )
    return df


def parse_runtime_to_minutes_col(col: F.Column) -> F.Column:
    """
    Parse runtime strings into integer minutes.
    Handles:
      '95'       -> 95
      '95m'      -> 95
      '1h 40m'   -> 100
      '2h'       -> 120
    """
    clean = F.trim(col.cast("string"))

    return (
        F.when(clean.rlike(r"^[0-9]+$"),
               clean.cast("int"))
         .when(clean.rlike(r"^[0-9]+m$"),
               F.regexp_extract(clean, r"([0-9]+)", 1).cast("int"))
         .when(clean.rlike(r"^[0-9]+h\s*[0-9]+m$"),
               (F.regexp_extract(clean, r"([0-9]+)h", 1).cast("int") * F.lit(60) +
                F.regexp_extract(clean, r"h\s*([0-9]+)m", 1).cast("int")))
         .when(clean.rlike(r"^[0-9]+h$"),
               F.regexp_extract(clean, r"([0-9]+)", 1).cast("int") * F.lit(60))
         .otherwise(F.lit(None).cast("int"))
    )


def parse_votes_to_int_col(col: F.Column) -> F.Column:
    """
    Parse votes strings into integer (BIGINT) counts.
    Handles:
      '123,456'    -> 123456
      '808582.0'   -> 808582
      '926K'       -> 926000
      '1.2M'       -> 1200000
    """
    raw = F.trim(col.cast("string"))
    no_commas = F.regexp_replace(raw, ",", "")

    return (
        # Simple integer or decimal (e.g. '808582' or '808582.0')
        F.when(
            no_commas.rlike(r"^[0-9]+(\.[0-9]+)?$"),
            no_commas.cast("double").cast("bigint")
        )
        # Thousands with K/k (e.g. '926K', '1.2K')
        .when(
            no_commas.rlike(r"^[0-9]+(\.[0-9]+)?[Kk]$"),
            (
                F.regexp_extract(no_commas, r"([0-9]+(?:\.[0-9]+)?)", 1)
                 .cast("double") * F.lit(1000)
            ).cast("bigint")
        )
        # Millions with M/m (e.g. '1.2M')
        .when(
            no_commas.rlike(r"^[0-9]+(\.[0-9]+)?[Mm]$"),
            (
                F.regexp_extract(no_commas, r"([0-9]+(?:\.[0-9]+)?)", 1)
                 .cast("double") * F.lit(1000000)
            ).cast("bigint")
        )
        .otherwise(F.lit(None).cast("bigint"))
    )


def filter_valid_title(df: DataFrame, title_col: str) -> DataFrame:
    """
    Remove junk records where the title is null, empty, or '[]'.
    """
    return df.filter(
        F.col(title_col).isNotNull() &
        (F.trim(F.col(title_col)) != "") &
        (F.trim(F.col(title_col)) != "[]")
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ##Silver – Netflix Movies

# COMMAND ----------


bronze_titles = spark.table("workspace.bronze.netflix_titles_raw")
bronze_credits = spark.table("workspace.bronze.netflix_credits_raw")

netflix_titles = rename_columns_to_camel(bronze_titles)
netflix_credits = rename_columns_to_camel(bronze_credits)

netflix_titles = clean_string_columns(netflix_titles)
netflix_credits = clean_string_columns(netflix_credits)

# Keep MOVIE type only
netflix_titles = netflix_titles.filter(F.upper(F.col("type")) == "MOVIE")

# Clean title and normalised title
netflix_titles = netflix_titles.withColumn(
    "titleClean",
    F.trim(F.regexp_replace(F.col("title"), r"^\s*[0-9]+\.\s*", ""))
)
netflix_titles = filter_valid_title(netflix_titles, "titleClean")
netflix_titles = netflix_titles.withColumn("titleNorm", F.lower(F.col("titleClean")))

# Normalise age certificate
netflix_titles = normalize_age_cert(netflix_titles)

# Cast numeric fields
netflix_titles = (
    netflix_titles
    .withColumn("releaseYear", F.col("releaseYear").cast("int"))
    .withColumn(
        "runtimeMinutes",
        F.when(F.col("runtime").rlike(r"^[0-9]+$"),
               F.col("runtime").cast("int"))
         .otherwise(F.lit(None).cast("int"))
    )
    .withColumn("imdbScore", F.col("imdbScore").cast("double"))
    .withColumn("imdbVotes", parse_votes_to_int_col(F.col("imdbVotes")))
    .withColumn("tmdbPopularity", F.col("tmdbPopularity").cast("double"))
    .withColumn("tmdbScore", F.col("tmdbScore").cast("double"))
)

# Director from credits: earliest personId for role DIRECTOR
netflix_directors = (
    netflix_credits
    .filter(F.upper(F.col("role")) == "DIRECTOR")
    .withColumn("personId", F.col("personId").cast("bigint"))
)

w_dir = Window.partitionBy("id").orderBy(F.col("personId").asc_nulls_last())
netflix_directors = (
    netflix_directors
    .withColumn("rn", F.row_number().over(w_dir))
    .filter(F.col("rn") == 1)
    .drop("rn")
    .select(
        "id",
        "personId",
        F.col("name").alias("director")
    )
)

# Join titles + directors
netflix_movies = (
    netflix_titles
    .join(netflix_directors, on="id", how="left")
)

# For same title + releaseYear, keep MOVIE with highest imdbScore (then imdbVotes)
w_netflix_title_year = (
    Window
    .partitionBy("titleNorm", "releaseYear")
    .orderBy(
        F.col("imdbScore").desc_nulls_last(),
        F.col("imdbVotes").desc_nulls_last()
    )
)
netflix_movies = (
    netflix_movies
    .withColumn("rn_title_year", F.row_number().over(w_netflix_title_year))
    .filter(F.col("rn_title_year") == 1)
    .drop("rn_title_year")
)

# Select final Netflix Silver columns
netflix_movies = netflix_movies.select(
    F.col("id").alias("netflixId"),
    F.col("titleClean").alias("title"),
    F.col("titleNorm").alias("titleNorm"),
    F.col("type").alias("showType"),
    F.col("description").alias("description"),
    F.col("releaseYear").alias("releaseYear"),
    F.col("ageCertification").alias("ageCertification"),
    F.col("runtimeMinutes").alias("runtimeMinutes"),
    F.col("genres").alias("genres"),
    F.col("productionCountries").alias("productionCountries"),
    F.col("imdbId").alias("imdbId"),
    F.col("imdbScore").alias("imdbScore"),
    F.col("imdbVotes").alias("imdbVotes"),
    F.col("tmdbPopularity").alias("tmdbPopularity"),
    F.col("tmdbScore").alias("tmdbScore"),
    F.col("director").alias("director")
)

netflix_movies = rename_columns_to_camel(netflix_movies)

netflix_movies.write.mode("overwrite").format("delta").saveAsTable(
    "workspace.silver.netflix_movies"
)


# COMMAND ----------

# MAGIC %md
# MAGIC ##Silver – IMDB Movies (from merged Bronze)

# COMMAND ----------


imdb_bronze = spark.table("workspace.bronze.imdb_merged_movies_raw")

imdb_df = rename_columns_to_camel(imdb_bronze)
imdb_df = clean_string_columns(imdb_df)

# Cast raw 'year'
imdb_df = imdb_df.withColumn("year", F.col("year").cast("int"))

# Derive releaseYear from releaseDate when possible, else fall back to 'year'.
imdb_df = imdb_df.withColumn(
    "releaseYearFromDate",
    F.expr("year(try_to_date(releaseDate, 'MMMM d, yyyy'))")
)
imdb_df = imdb_df.withColumn(
    "releaseYear",
    F.when(F.col("releaseYearFromDate").isNotNull(),
           F.col("releaseYearFromDate").cast("int"))
     .otherwise(F.col("year"))
)

# Clean title + normalised title
imdb_df = imdb_df.withColumn(
    "titleClean",
    F.trim(F.regexp_replace(F.col("title"), r"^\s*[0-9]+\.\s*", ""))
)
imdb_df = filter_valid_title(imdb_df, "titleClean")
imdb_df = imdb_df.withColumn("titleNorm", F.lower(F.col("titleClean")))

# Parse runtime from 'duration'
if "duration" in imdb_df.columns:
    imdb_df = imdb_df.withColumn(
        "runtimeMinutes",
        parse_runtime_to_minutes_col(F.col("duration"))
    )
else:
    imdb_df = imdb_df.withColumn("runtimeMinutes", F.lit(None).cast("int"))

# IMDB rating numeric (this is the official IMDB rating from IMDB dataset)
if "rating" in imdb_df.columns:
    imdb_df = imdb_df.withColumn("imdbRating", F.col("rating").cast("double"))
else:
    imdb_df = imdb_df.withColumn("imdbRating", F.lit(None).cast("double"))

# IMDB votes as BIGINT (priority dataset for votes)
if "votes" in imdb_df.columns:
    imdb_df = imdb_df.withColumn("imdbVotes", parse_votes_to_int_col(F.col("votes")))
else:
    imdb_df = imdb_df.withColumn("imdbVotes", F.lit(None).cast("bigint"))

# imdbId from movieLink
if "movieLink" in imdb_df.columns:
    imdb_df = imdb_df.withColumn(
        "imdbId",
        F.regexp_extract(F.col("movieLink"), r"(tt[0-9]+)", 0)
    )
else:
    imdb_df = imdb_df.withColumn("imdbId", F.lit(None).cast("string"))

# Deduplicate IMDB: one row per (titleNorm, releaseYear) with highest rating then votes
w_imdb = (
    Window.partitionBy("titleNorm", "releaseYear")
          .orderBy(
              F.col("imdbRating").desc_nulls_last(),
              F.col("imdbVotes").desc_nulls_last()
          )
)
imdb_df = (
    imdb_df
    .withColumn("rn", F.row_number().over(w_imdb))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

imdb_movies = imdb_df.select(
    F.col("titleClean").alias("title"),
    F.col("titleNorm").alias("titleNorm"),
    F.col("releaseYear").alias("releaseYear"),
    F.col("runtimeMinutes").alias("runtimeMinutes"),
    F.col("imdbRating").alias("imdbRating"),
    F.col("imdbVotes").alias("imdbVotes"),
    F.col("imdbId").alias("imdbId"),
    F.col("fileYear").alias("fileYear")
)

imdb_movies = rename_columns_to_camel(imdb_movies)

imdb_movies.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .format("delta") \
    .saveAsTable("workspace.silver.imdb_movies")



# COMMAND ----------

# MAGIC %md
# MAGIC # Combined Silver movies table

# COMMAND ----------

netflix_movies = spark.table("workspace.silver.netflix_movies")
imdb_movies = spark.table("workspace.silver.imdb_movies")

n = netflix_movies.alias("n")
i = imdb_movies.alias("i")

# Join:
# 1) primary on imdbId (generated imdbid from Link colun)
# 2) fallback on (titleNorm, releaseYear) when imdbId is null
join_condition = (
    (F.col("n.imdbId").isNotNull() & (F.col("n.imdbId") == F.col("i.imdbId"))) |
    (F.col("n.imdbId").isNull() &
     (F.col("n.titleNorm") == F.col("i.titleNorm")) &
     (F.col("n.releaseYear") == F.col("i.releaseYear")))
)

movies_joined = n.join(i, join_condition, how="left")

# --- Final rating & votes ---
# Rating: IMDB dataset first; if that doesn't exist, use Netflix imdbScore as fallback
movies_joined = (
    movies_joined
    .withColumn(
        "imdbRatingFinal",
        F.coalesce(F.col("i.imdbRating"), F.col("n.imdbScore").cast("double"))
    )
    # Votes: IMDB dataset first. if that doesn't exist then fall back to Netflix imdbVotes
    .withColumn(
        "imdbVotesFinal",
        F.coalesce(F.col("i.imdbVotes"), F.col("n.imdbVotes").cast("bigint"))
    )
    .withColumn(
        "hasImdbMatch",
        F.when(F.col("i.imdbId").isNotNull(), F.lit(True)).otherwise(F.lit(False))
    )
)

final_movies = movies_joined.select(
    F.col("n.netflixId").alias("netflixId"),
    F.col("n.title").alias("title"),
    F.col("n.releaseYear").alias("releaseYear"),
    F.col("n.runtimeMinutes").alias("runtimeMinutes"),
    F.col("n.ageCertification").alias("ageCertification"),
    F.col("n.director").alias("director"),
    F.col("n.genres").alias("genres"),
    F.col("n.productionCountries").alias("productionCountries"),
    F.col("n.showType").alias("showType"),
    F.col("n.imdbId").alias("imdbId"),
    F.col("imdbRatingFinal").alias("imdbRating"),
    F.col("imdbVotesFinal").alias("imdbVotes"),
    F.col("n.tmdbPopularity").alias("tmdbPopularity"),
    F.col("n.tmdbScore").alias("tmdbScore"),
    F.col("hasImdbMatch").alias("hasImdbMatch")
)

# Remove any remaining junk titles 
final_movies = filter_valid_title(final_movies, "title")

# Columns to camelCase
final_movies = rename_columns_to_camel(final_movies)

final_movies.write.mode("overwrite").format("delta").saveAsTable(
    "workspace.silver.movies"
)
