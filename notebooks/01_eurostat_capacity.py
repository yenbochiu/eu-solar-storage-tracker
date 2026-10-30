# Databricks notebook source
print("hello")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS workspace.energy_tracker;
# MAGIC CREATE VOLUME IF NOT EXISTS workspace.energy_tracker.raw;

# COMMAND ----------

import requests
urls = {
    "ENTSO-E":  "https://web-api.tp.entsoe.eu/api",
    "Eurostat": "https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/dataflow/ESTAT/nrg_inf_epcrw",
}
for name, u in urls.items():
    try:
        r = requests.get(u, timeout=15)
        print(f"{name}: HTTP {r.status_code}")   # 401/400 也代表連得上
    except Exception as e:
        print(f"{name}: BLOCKED ({type(e).__name__})")

# COMMAND ----------

# bronze delta table 
# 第一個 PySpark DataFrame → Delta table
# 用 Eurostat 的再生能源裝機量資料集 nrg_inf_epcrw

import io, pandas as pd
from pyspark.sql import functions as F

url = ("https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/data/"
       "nrg_inf_epcrw/?format=SDMX-CSV")
pdf = pd.read_csv(io.StringIO(requests.get(url, timeout=120).text), dtype=str)
pdf.columns = [c.strip().lower().replace(" ", "_") for c in pdf.columns]
print(pdf.columns.tolist())          # 先看有哪些欄位

df = spark.createDataFrame(pdf)      # pandas → Spark
solar = (df.filter(F.col("siec") == "RA420")          # RA420 = 太陽能 PV
           .withColumn("obs_value", F.col("obs_value").cast("double"))
           .withColumn("ingested_at", F.current_timestamp()))

solar.write.mode("overwrite").saveAsTable(
    "workspace.energy_tracker.bronze_eurostat_solar_capacity")
display(solar.limit(10))

# plant_tec - CAP_NET_ELC 淨裝機容量 

# COMMAND ----------

# 探索 plant_tec 的值全部有哪些
bronze = spark.table("workspace.energy_tracker.bronze_eurostat_solar_capacity")
display(bronze.groupBy("plant_tec", "unit").count())

# COMMAND ----------

from pyspark.sql import functions as F

silver = (bronze
    .filter(F.col("plant_tec") == "CAP_NET_ELC") # 確認只看這個值
    .select(                                              # = SQL SELECT ... AS
        F.col("geo").alias("country"), # 有國家個別數據和整個歐盟的數據要特別注意
        F.col("time_period").cast("int").alias("year"), # 字串轉成 timestamp
        F.col("obs_value").alias("solar_pv_capacity_mw"),
        F.col("obs_flag"),
        F.to_timestamp("last_update", "dd/MM/yy HH:mm:ss").alias("source_updated_at"),
        F.col("ingested_at"),
    )
    .filter(F.col("solar_pv_capacity_mw").isNotNull())    # = WHERE ... IS NOT NULL
    .filter(F.col("year") >= 2015) # 保留2015之後的資料(因為觀察上一個bronze table發現1990年代幾乎都是0)
    .withColumn("solar_pv_capacity_gw",                   # 新增欄位
                F.round(F.col("solar_pv_capacity_mw") / 1000, 2))
)

silver.write.mode("overwrite").saveAsTable(
    "workspace.energy_tracker.silver_eurostat_solar_capacity")
display(silver.limit(10))

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH s AS (
# MAGIC   SELECT * FROM workspace.energy_tracker.silver_eurostat_solar_capacity
# MAGIC   WHERE country NOT LIKE 'EU%' AND country NOT LIKE 'EA%'
# MAGIC )
# MAGIC SELECT country, year, solar_pv_capacity_gw
# MAGIC FROM s
# MAGIC WHERE year = (SELECT MAX(year) FROM s)
# MAGIC ORDER BY solar_pv_capacity_gw DESC
# MAGIC LIMIT 10;

# COMMAND ----------

