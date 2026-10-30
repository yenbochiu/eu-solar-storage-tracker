# Databricks notebook source
# MAGIC %pip install entsoe-py
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import pandas as pd
from entsoe import EntsoePandasClient
from pyspark.sql import functions as F

dbutils.widgets.text("days_back", "14")              # Job 可以傳參數進來
DAYS  = int(dbutils.widgets.get("days_back"))
T     = "workspace.energy_tracker."
ZONES = ["NL", "DE_LU", "BE", "FR", "ES"]
client = EntsoePandasClient(api_key=dbutils.secrets.get("energy", "entsoe_token"))
end   = pd.Timestamp.now(tz="Europe/Brussels").normalize()
start = end - pd.Timedelta(days=DAYS)

def to_utc_naive(s):
    return pd.to_datetime(s, utc=True).dt.tz_localize(None)

def table_exists(table):
    """直接嘗試讀表：讀得到就存在，比 catalog 快取可靠"""
    try:
        spark.table(table).limit(0).collect()
        return True
    except Exception:
        return False

def upsert(pdf, table, keys):
    pdf = pdf.drop_duplicates(subset=keys)
    sdf = spark.createDataFrame(pdf)
    if not table_exists(table):                       # ← 改用新的檢查方式
        sdf.write.saveAsTable(table); print(table, "created", len(pdf)); return
    sdf.createOrReplaceTempView("updates")
    on = " AND ".join(f"t.{k} = s.{k}" for k in keys)
    spark.sql(f"""MERGE INTO {table} t USING updates s ON {on}
                  WHEN MATCHED THEN UPDATE SET *
                  WHEN NOT MATCHED THEN INSERT *""")
    print(table, "merged", len(pdf))

# COMMAND ----------

def gen_series(g):
    """把 query_generation 的結果統一成一條 Series（MW）"""
    if isinstance(g, pd.Series):
        return g
    g.columns = ["_".join(c) if isinstance(c, tuple) else c for c in g.columns]
    return g[[c for c in g.columns if "Consumption" not in c][0]]

def finish(frames):
    df = pd.concat(frames); df["ts_utc"] = to_utc_naive(df.pop("ts")); return df

def fetch_prices():
    out = []
    for z in ZONES:
        try:
            s = client.query_day_ahead_prices(z, start=start, end=end)
            p = s.rename("price_eur_mwh").rename_axis("ts").reset_index(); p["zone"] = z; out.append(p)
        except Exception as e:
            print(z, "price FAILED", e)
    return finish(out)

def fetch_gen(psr_list, col):
    out = []
    for z in ZONES:
        parts = []
        for psr in psr_list:
            try:
                s = gen_series(client.query_generation(z, start=start, end=end, psr_type=psr))
                parts.append(s.resample("1h").mean())    # ← 新增：先統一成每小時
            except Exception as e:
                print(z, psr, "skip:", type(e).__name__)
        if parts:
            s = pd.concat(parts, axis=1).sum(axis=1, min_count=1)
            d = s.rename(col).rename_axis("ts").reset_index(); d["zone"] = z; out.append(d)
    return finish(out)

upsert(fetch_prices(),                      T + "bronze_entsoe_da_price",  ["zone", "ts_utc"])
upsert(fetch_gen(["B16"], "solar_mw"),      T + "bronze_entsoe_solar_gen", ["zone", "ts_utc"])
upsert(fetch_gen(["B18", "B19"], "wind_mw"), T + "bronze_entsoe_wind_gen", ["zone", "ts_utc"])  # 離岸 + 陸上

# COMMAND ----------

# 1) 風電表存在嗎？有資料嗎？
wind_ok = spark.catalog.tableExists(T + "bronze_entsoe_wind_gen")
print("wind table exists:", wind_ok)
if wind_ok:
    display(spark.sql(f"""
        SELECT zone, COUNT(*) AS n, COUNT(wind_mw) AS non_null, MIN(ts_utc), MAX(ts_utc)
        FROM {T}bronze_entsoe_wind_gen GROUP BY zone"""))

# 2) 直接抓一次 NL 陸上風電，看錯誤訊息
try:
    g = client.query_generation("NL", start=start, end=end, psr_type="B19")
    print(type(g))
    print(g.head())
except Exception as e:
    print("ERROR:", type(e).__name__, e)

# COMMAND ----------

from pyspark.sql import functions as F

def hourly_avg(table, src, dst):
    return (spark.table(T + table)
        .withColumn("hour", F.date_trunc("hour", "ts_utc"))
        .groupBy("zone", "hour").agg(F.avg(src).alias(dst)))

hourly = (hourly_avg("bronze_entsoe_da_price",  "price_eur_mwh", "price")
    .join(hourly_avg("bronze_entsoe_solar_gen", "solar_mw", "solar_mwh"), ["zone", "hour"], "left")
    .join(hourly_avg("bronze_entsoe_wind_gen",  "wind_mw",  "wind_mwh"),  ["zone", "hour"], "left"))
hourly.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(T + "silver_entsoe_hourly")

weekly = (hourly.withColumn("week_start", F.date_trunc("week", "hour").cast("date"))
    .groupBy("zone", "week_start")
    .agg(
        F.count("*").alias("hours_covered"),
        F.round(F.avg("price"), 1).alias("avg_price_eur_mwh"),
        F.round(F.min("price"), 1).alias("min_price"),
        F.round(F.max("price"), 1).alias("max_price"),
        F.sum(F.when(F.col("price") < 0, 1).otherwise(0)).alias("negative_price_hours"),
        F.round(F.sum("solar_mwh") / 1000, 1).alias("solar_gwh"),
        F.round(F.sum("wind_mwh") / 1000, 1).alias("wind_gwh"),
        F.round(F.sum(F.col("price") * F.col("solar_mwh")) / F.sum("solar_mwh"), 1).alias("solar_capture_price"),
    )
    .withColumn("solar_capture_rate", F.round(F.col("solar_capture_price") / F.col("avg_price_eur_mwh"), 2)))

daily_spread = (hourly.groupBy("zone", F.to_date("hour").alias("day"))
    .agg((F.max("price") - F.min("price")).alias("spread"))
    .withColumn("week_start", F.date_trunc("week", "day").cast("date"))
    .groupBy("zone", "week_start").agg(F.round(F.avg("spread"), 1).alias("avg_daily_spread")))

(weekly.join(daily_spread, ["zone", "week_start"])
    .write.mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable(T + "gold_weekly_market"))

# COMMAND ----------

from pyspark.sql import Window

EFF = 0.88      # 來回效率 (round-trip efficiency)
H   = 2         # 1 MW / 2 MWh → 每天充 2 小時、放 2 小時

day_cheap = Window.partitionBy("zone", "day").orderBy("price")
day_pricy = Window.partitionBy("zone", "day").orderBy(F.desc("price"))

daily_bess = (spark.table(T + "silver_entsoe_hourly")
    .filter(F.col("price").isNotNull())
    .withColumn("day", F.to_date("hour"))
    .withColumn("r_low",  F.row_number().over(day_cheap))   # 當天第幾便宜
    .withColumn("r_high", F.row_number().over(day_pricy))   # 當天第幾貴
    .groupBy("zone", "day")
    .agg(F.count("*").alias("hours"),
         F.sum(F.when(F.col("r_low")  <= H, F.col("price"))).alias("charge_cost"),
         F.sum(F.when(F.col("r_high") <= H, F.col("price"))).alias("discharge_value"))
    .filter("hours >= 20")
    .withColumn("profit_eur_per_mw",          # 價差不夠就不充放電 → 0
        F.greatest(F.lit(0.0), F.col("discharge_value") * EFF - F.col("charge_cost"))))

bess_weekly = (daily_bess
    .withColumn("week_start", F.date_trunc("week", "day").cast("date"))
    .groupBy("zone", "week_start")
    .agg(F.round(F.sum("profit_eur_per_mw"), 0).alias("bess_eur_per_mw_week"))
    .withColumn("bess_eur_per_mw_year_run_rate", F.col("bess_eur_per_mw_week") * 52))

bess_weekly.write.mode("overwrite").saveAsTable(T + "gold_weekly_bess")
display(bess_weekly.orderBy(F.desc("week_start"), F.desc("bess_eur_per_mw_week")))

# COMMAND ----------

by_zone_time = Window.partitionBy("zone").orderBy("week_start")
by_zone_bess = Window.partitionBy("zone").orderBy("bess_eur_per_mw_week")
by_week      = Window.partitionBy("week_start").orderBy(F.desc("bess_eur_per_mw_week"))

signals = (spark.table(T + "gold_weekly_market")
    .join(spark.table(T + "gold_weekly_bess"), ["zone", "week_start"], "left")
    .filter("hours_covered >= 120")
    # 和自己的歷史比：這週的儲能收益落在第幾百分位？
    .withColumn("bess_percentile", F.round(F.percent_rank().over(by_zone_bess), 2))
    # 和其他市場比：這週哪個市場最適合儲能？
    .withColumn("bess_rank_this_week", F.rank().over(by_week))
    # 電價週變化 %
    .withColumn("price_wow_pct", F.round(
        (F.col("avg_price_eur_mwh") / F.lag("avg_price_eur_mwh").over(by_zone_time) - 1) * 100, 1))
    .withColumn("storage_signal",
        F.when(F.col("bess_percentile") >= 0.75, "🟢 Strong")
         .when(F.col("bess_percentile") <= 0.25, "🔴 Weak")
         .otherwise("🟡 Normal"))
    .withColumn("solar_signal",
        F.when(F.col("solar_capture_rate") < 0.7, "🔴 Cannibalization")
         .when(F.col("solar_capture_rate") < 0.9, "🟡 Watch")
         .otherwise("🟢 Healthy"))
    .withColumn("action", F.concat_ws(" | ",
        F.when(F.col("storage_signal").startswith("🟢"),
               F.lit("Storage: spreads above own history → deep-dive this market")),
        F.when(F.col("solar_signal").startswith("🔴"),
               F.lit("Solar: low capture → pair with storage / check PPA pricing")),
        F.when(F.col("negative_price_hours") >= 20,
               F.lit("Grid: frequent negative prices → flexibility demand rising")),
        F.when(F.abs(F.col("price_wow_pct")) >= 20,
               F.lit("Price: big weekly move → check wind / gas / outages")))))

signals.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(T + "gold_weekly_signals")
display(signals.orderBy(F.desc("week_start"), "bess_rank_this_week"))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT zone, week_start, solar_gwh, wind_gwh
# MAGIC FROM workspace.energy_tracker.gold_weekly_signals
# MAGIC ORDER BY week_start DESC, zone
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT zone, ROUND(AVG(wind_mw)) AS avg_mw, MAX(wind_mw) AS max_mw
# MAGIC FROM workspace.energy_tracker.bronze_entsoe_wind_gen
# MAGIC GROUP BY zone

# COMMAND ----------

s = pd.Timestamp("2026-10-20", tz="Europe/Brussels")
e = pd.Timestamp("2026-10-22", tz="Europe/Brussels")

# A) 只抓陸上風電 (B19)：看原始欄位和數值
g = client.query_generation("ES", start=s, end=e, psr_type="B19")
print("TYPE:", type(g))
print("COLUMNS:", list(g.columns) if hasattr(g, "columns") else g.name)
print(g.describe())

# B) 不指定類型，抓全部：看有沒有含 Wind 的欄位
allg = client.query_generation("ES", start=s, end=e)
wind_cols = [c for c in allg.columns if "Wind" in str(c)]
print("WIND COLS (all types):", wind_cols)
print(allg[wind_cols].describe())

# COMMAND ----------

