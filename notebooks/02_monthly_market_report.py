# Databricks notebook source
# MAGIC %md
# MAGIC # Monthly market report data (all European bidding zones)
# MAGIC
# MAGIC Feeds the "Energy price trend" pages of the EU Digital Power monthly report and the
# MAGIC Market Insight Tracker's country scorecard:
# MAGIC
# MAGIC | Report chart | Column |
# MAGIC |---|---|
# MAGIC | Day-ahead spot price trend | `avg_price_eur_mwh` |
# MAGIC | Hours with zero and negative prices | `zero_or_negative_hours` |
# MAGIC | Monthly average max daily spread | `avg_daily_spread` |
# MAGIC | (new) Storage attractiveness | `bess_eur_per_mw_month`, `bess_rank` |
# MAGIC
# MAGIC Prices only (cheap API calls), so it can cover every zone the report shows.
# MAGIC Public ENTSO-E data only.

# COMMAND ----------

# MAGIC %pip install entsoe-py
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import pandas as pd
from entsoe import EntsoePandasClient
from pyspark.sql import functions as F, Window

dbutils.widgets.text("months_back", "2")      # backfill once with 24, then 2 is enough for the monthly job
MONTHS = int(dbutils.widgets.get("months_back"))
T = "workspace.energy_tracker."

# Every zone that appears in the monthly report (negative-hours chart + spreads chart)
ZONES = [
    "DE_LU", "NL", "BE", "FR", "AT", "ES", "PT",
    "IT_NORD", "IT_CNOR", "IT_CSUD", "IT_SUD", "IT_SICI", "IT_SARD", "IT_CALA",
    "PL", "CZ", "SK", "HU", "RO", "BG", "GR", "HR", "SI",
    "SE_1", "SE_2", "SE_3", "SE_4", "DK_1", "DK_2", "FI",
    "EE", "LV", "LT", "IE_SEM",
]

client = EntsoePandasClient(api_key=dbutils.secrets.get("energy", "entsoe_token"))
end   = pd.Timestamp.now(tz="Europe/Brussels").normalize()
start = (end - pd.DateOffset(months=MONTHS)).replace(day=1)

def table_exists(table):
    try:
        spark.table(table).limit(0).collect(); return True
    except Exception:
        return False

def upsert(pdf, table, keys):
    pdf = pdf.drop_duplicates(subset=keys)
    sdf = spark.createDataFrame(pdf)
    if not table_exists(table):
        sdf.write.saveAsTable(table); print(table, "created", len(pdf)); return
    sdf.createOrReplaceTempView("updates")
    on = " AND ".join(f"t.{k} = s.{k}" for k in keys)
    spark.sql(f"""MERGE INTO {table} t USING updates s ON {on}
                  WHEN MATCHED THEN UPDATE SET *
                  WHEN NOT MATCHED THEN INSERT *""")
    print(table, "merged", len(pdf))

# COMMAND ----------

# Bronze: day-ahead prices for all zones (15-min or hourly, as published)
frames, failed = [], []
for z in ZONES:
    try:
        s = client.query_day_ahead_prices(z, start=start, end=end)
        p = s.rename("price_eur_mwh").rename_axis("ts").reset_index()
        p["ts_utc"] = pd.to_datetime(p.pop("ts"), utc=True).dt.tz_localize(None)
        p["zone"] = z
        frames.append(p)
    except Exception as e:
        failed.append(z); print("skip", z, type(e).__name__)

upsert(pd.concat(frames), T + "bronze_entsoe_da_price_all", ["zone", "ts_utc"])
print("zones failed:", failed)

# COMMAND ----------

# Silver: normalise to hourly (15-min zones averaged), so hour counts match the report
hourly = (spark.table(T + "bronze_entsoe_da_price_all")
    .withColumn("hour", F.date_trunc("hour", "ts_utc"))
    .groupBy("zone", "hour").agg(F.avg("price_eur_mwh").alias("price")))
hourly.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(T + "silver_price_hourly_all")

# COMMAND ----------

# Gold: monthly KPIs per zone
EFF, H = 0.88, 2   # same 1 MW / 2 MWh screening battery as the weekly pipeline

day_cheap = Window.partitionBy("zone", "day").orderBy("price")
day_pricy = Window.partitionBy("zone", "day").orderBy(F.desc("price"))

daily = (hourly.filter(F.col("price").isNotNull())
    .withColumn("day", F.to_date("hour"))
    .withColumn("r_low",  F.row_number().over(day_cheap))
    .withColumn("r_high", F.row_number().over(day_pricy))
    .groupBy("zone", "day")
    .agg(F.count("*").alias("hours"),
         (F.max("price") - F.min("price")).alias("daily_spread"),
         F.sum(F.when(F.col("r_low")  <= H, F.col("price"))).alias("charge_cost"),
         F.sum(F.when(F.col("r_high") <= H, F.col("price"))).alias("discharge_value"))
    .filter("hours >= 20")
    .withColumn("bess_profit", F.greatest(F.lit(0.0), F.col("discharge_value") * EFF - F.col("charge_cost")))
    .withColumn("month", F.date_trunc("month", "day").cast("date")))

monthly_daily = (daily.groupBy("zone", "month")
    .agg(F.count("*").alias("days_covered"),
         F.round(F.avg("daily_spread"), 1).alias("avg_daily_spread"),
         F.round(F.sum("bess_profit"), 0).alias("bess_eur_per_mw_month")))

monthly_hourly = (hourly.withColumn("month", F.date_trunc("month", "hour").cast("date"))
    .groupBy("zone", "month")
    .agg(F.round(F.avg("price"), 1).alias("avg_price_eur_mwh"),
         F.sum(F.when(F.col("price") <= 0, 1).otherwise(0)).alias("zero_or_negative_hours"),
         F.sum(F.when(F.col("price") < 0, 1).otherwise(0)).alias("negative_hours")))

by_zone  = Window.partitionBy("zone").orderBy("month")
by_month = Window.partitionBy("month").orderBy(F.desc("bess_eur_per_mw_month"))

monthly = (monthly_hourly.join(monthly_daily, ["zone", "month"])
    .filter("days_covered >= 25")                                   # drop partial months
    .withColumn("bess_rank", F.rank().over(by_month))
    .withColumn("price_mom_pct", F.round((F.col("avg_price_eur_mwh") / F.lag("avg_price_eur_mwh").over(by_zone) - 1) * 100, 1))
    .withColumn("price_yoy_pct", F.round((F.col("avg_price_eur_mwh") / F.lag("avg_price_eur_mwh", 12).over(by_zone) - 1) * 100, 1))
    .withColumn("spread_yoy_pct", F.round((F.col("avg_daily_spread") / F.lag("avg_daily_spread", 12).over(by_zone) - 1) * 100, 1)))

monthly.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(T + "gold_monthly_market")
display(monthly.orderBy(F.desc("month"), "bess_rank"))

# COMMAND ----------

# Export the latest 14 months as CSV for the monthly report and the tracker Sheet
spark.sql("CREATE VOLUME IF NOT EXISTS workspace.energy_tracker.exports")
out = (spark.table(T + "gold_monthly_market")
       .filter(F.col("month") >= F.add_months(F.trunc(F.current_date(), "month"), -14))
       .orderBy("month", "zone").toPandas())
path = "/Volumes/workspace/energy_tracker/exports/monthly_market.csv"
out.to_csv(path, index=False)
print("saved", path, len(out), "rows")

# COMMAND ----------

# Publish the same CSV to GitHub (data/monthly_market.csv) so the scheduled Claude task
# can update Market_Data and Country_Scorecard in the tracker Sheet without a manual upload.
# One-time setup: a fine-grained GitHub token with "Contents: Read and write" on this repo only,
# stored as Databricks secret scope "energy", key "github_token". Without it this step is skipped.
import base64, requests

REPO, GH_PATH = "yenbochiu/eu-solar-storage-tracker", "data/monthly_market.csv"
try:
    gh_token = dbutils.secrets.get("energy", "github_token")
except Exception:
    gh_token = None
    print("Secret energy/github_token not found - skipped GitHub upload")

if gh_token:
    api = f"https://api.github.com/repos/{REPO}/contents/{GH_PATH}"
    headers = {"Authorization": f"Bearer {gh_token}", "Accept": "application/vnd.github+json"}
    current = requests.get(api, headers=headers, params={"ref": "main"}, timeout=30)
    body = {
        "message": f"Update monthly_market.csv ({pd.Timestamp.now(tz='Europe/Amsterdam'):%Y-%m-%d}, {len(out)} rows)",
        "content": base64.b64encode(out.to_csv(index=False).encode("utf-8")).decode(),
        "branch": "main",
    }
    if current.status_code == 200:
        body["sha"] = current.json()["sha"]          # required to overwrite the previous month's file
    resp = requests.put(api, headers=headers, json=body, timeout=60)
    resp.raise_for_status()                          # fail the job loudly if the upload is rejected
    print("pushed", GH_PATH, "to", REPO, resp.json()["commit"]["sha"][:7])
