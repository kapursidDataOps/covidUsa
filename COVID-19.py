# Databricks notebook source
# MAGIC %run ./functions

# COMMAND ----------

# DBTITLE 1,Read Dataset
dfCovid = spark.read.format("parquet").load(wasbs_path)
dfStates = spark.read.option("header", "true").parquet(parquetUsState)

# COMMAND ----------

# DBTITLE 1,Getting Full State Name for Covid Analysis
dfCovidStates = dfCovid.alias("c").join(
    dfStates.withColumn("Us_States", col("State")),
    col("c.state") == col("stateCode"),
    "inner",
)

# COMMAND ----------

# DBTITLE 1,Covid Cases in Feb
dfFeb = (
    dfCovidStates.select("date", "Us_States", "positive")
    .distinct()
    .filter(col("date").like("%2021-02%"))
    .orderBy(col("date").desc(), col("Us_States"))
)

# COMMAND ----------

# DBTITLE 1,Covid Cases in Jan
dfJan = (
    dfCovidStates.select("date", "Us_States", "positive")
    .distinct()
    .filter(col("date").like("%2021-01%"))
    .orderBy(col("date").desc(), col("Us_States"))
)


# COMMAND ----------

# DBTITLE 1,TotalPositiveCases
dfFinalJan = (
    dfJan.groupBy("Us_States")
    .agg(sum("positive").alias("TotalPositiveInJan"))
    .orderBy("Us_States")
)
dfFinalFeb = (
    dfFeb.groupBy("Us_States")
    .agg(sum("positive").alias("TotalPositiveInFeb"))
    .orderBy("Us_States")
)


# COMMAND ----------

# DBTITLE 1,Monthly Count Difference in Cases
dfCasesDifferenceJF = (
    dfFinalFeb.alias("f")
    .join(
        dfFinalJan.alias("j"),
        col("j.Us_States") == col("f.Us_States"),
    )
    .select(
        "j.Us_States",
        col("TotalPositiveInFeb").alias("CurrentCases"),
        col("TotalPositiveInJan").alias("PreviousCases"),
    )
    .orderBy(col("Us_States"))
)

# COMMAND ----------

# DBTITLE 1,Writing to ADLS
dfCasesDifferenceJF.write.format("parquet").save(JanFebCovid)

# COMMAND ----------


