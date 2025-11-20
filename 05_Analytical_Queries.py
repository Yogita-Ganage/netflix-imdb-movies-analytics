# Databricks notebook source
# MAGIC %md
# MAGIC ####Number of titles released per year

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   releaseYear,
# MAGIC   COUNT(*) AS titleCount
# MAGIC FROM gold.movies
# MAGIC GROUP BY releaseYear
# MAGIC ORDER BY releaseYear,count(*) DESC;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ####Top directors by content production

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   director,
# MAGIC   COUNT(*)        AS titleCount,
# MAGIC   ROUND(AVG(imdbRating), 2) AS avgImdbRating
# MAGIC FROM gold.movies
# MAGIC WHERE director IS NOT NULL
# MAGIC GROUP BY director
# MAGIC ORDER BY titleCount DESC, avgImdbRating DESC
# MAGIC LIMIT 20;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ####Most common age certifications

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   ageCertification,
# MAGIC   COUNT(*) AS titleCount
# MAGIC FROM gold.movies
# MAGIC GROUP BY ageCertification
# MAGIC ORDER BY titleCount DESC;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog `workspace`; select * from `gold`.`movies` limit 100;

# COMMAND ----------

# MAGIC %md
# MAGIC
