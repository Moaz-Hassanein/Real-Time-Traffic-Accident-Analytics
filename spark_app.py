import pymysql
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, when, count
from pyspark.sql.types import StructType, StringType, IntegerType, DoubleType
from pyspark.sql import functions as F
import os
import shutil
import time

checkpoint_dirs = ["C:/BigDataProject/narrow", "C:/BigDataProject/checkpoints"]
for d in checkpoint_dirs:
    if os.path.exists(d):
        shutil.rmtree(d)
    os.makedirs(d, exist_ok=True)
# last_batch_time = {"narrow": time.time(), "wide": time.time()}
# warning_sent = {"narrow": False, "wide": False}

# def check_inactivity():
#     for stream in ["narrow", "wide"]:
#         if time.time() - last_batch_time[stream] > 60 and not warning_sent[stream]:
#             print(f"No batches for {stream} stream for 60 seconds — stream seems inactive.")
#             warning_sent[stream] = True
#         elif time.time() - last_batch_time[stream] <= 60:
#             warning_sent[stream] = False

spark = SparkSession.builder \
    .appName("TrafficStreamingAnalysis") \
    .master("local[*]") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1") \
    .config("spark.driver.extraClassPath", "C:\\spark\\jars\\mysql-connector-j-8.3.0.jar") \
    .config("spark.executor.extraClassPath", "C:\\spark\\jars\\mysql-connector-j-8.3.0.jar") \
    .config("spark.sql.shuffle.partitions", "2") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")
schema = StructType() \
    .add("ID", IntegerType()) \
    .add("Country", StringType()) \
    .add("Year", StringType()) \
    .add("Month", StringType()) \
    .add("Day_of_Week", StringType()) \
    .add("Time_of_Day", StringType()) \
    .add("Urban_Rural", StringType()) \
    .add("Road_Type", StringType()) \
    .add("Weather_Conditions", StringType()) \
    .add("Speed_Limit", StringType()) \
    .add("Driver_Alcohol_Level", DoubleType()) \
    .add("Driver_Fatigue", IntegerType()) \
    .add("Vehicle_Condition", StringType()) \
    .add("Traffic_Volume", IntegerType()) \
    .add("Road_Condition", StringType()) \
    .add("Accident_Cause", StringType()) \
    .add("Region", StringType()) \
    .add("Vehicle_Type", StringType())

df_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "test4") \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load()

df_parsed = df_stream.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

total_inserted_narrow = 0
EXPECTED_TOTAL = 5500

df_narrow = df_parsed.select(
    col("ID").alias("id"),
    col("Region").alias("region"),
    col("Time_of_Day").alias("time_of_day"),
    col("Driver_Alcohol_Level").alias("driver_alcohol_level"),
    col("Accident_Cause").alias("accident_cause")
).withColumn(
    "alcohol_risk",
    when(col("driver_alcohol_level") > 0.05, "High Risk").otherwise("Low Risk")
)

def write_narrow(batch_df, batch_id):
    global total_inserted_narrow
    # last_batch_time["narrow"] = time.time()
    rows = batch_df.collect()
    if not rows:
        return
    data = [(int(r["id"]), r["region"] or "Unknown", r["time_of_day"] or "Unknown",
            float(r["driver_alcohol_level"] or 0.0), r["accident_cause"] or "Unknown",
            r["alcohol_risk"]) for r in rows]
    try:
        conn = pymysql.connect(host="localhost", user="admin", password="1234", database="traffic_analysis")
        query = """
        INSERT INTO alcohol_risk_analysis
        (id, region, time_of_day, driver_alcohol_level, accident_cause, alcohol_risk, batch_time)
        VALUES (%s, %s, %s, %s, %s, %s, NOW())
        ON DUPLICATE KEY UPDATE
            region=VALUES(region), time_of_day=VALUES(time_of_day),
            driver_alcohol_level=VALUES(driver_alcohol_level),
            accident_cause=VALUES(accident_cause),
            alcohol_risk=VALUES(alcohol_risk),
            batch_time=VALUES(batch_time)
        """
        with conn.cursor() as cursor:
            cursor.executemany(query, data)
            conn.commit()
            total_inserted_narrow += cursor.rowcount
        conn.close()
    except Exception as e:
        print("Error in narrow batch:", e)

narrow_query = df_narrow.writeStream \
    .outputMode("append") \
    .foreachBatch(write_narrow) \
    .option("checkpointLocation", "C:/BigDataProject/narrow") \
    .trigger(processingTime="20 seconds") \
    .start()
aggregation_configs = [
    {"table": "accidents_by_region", "group_by": "Region", "db_column": "region"},
    {"table": "accidents_by_urban_rural", "group_by": "Urban_Rural", "db_column": "urban_rural"},
    {"table": "accidents_by_road_type", "group_by": "Road_Type", "db_column": "road_type"},
    {"table": "accidents_by_country", "group_by": "Country", "db_column": "country"},
    {"table": "top_accident_causes", "group_by": "Accident_Cause", "db_column": "accident_cause"},
    {"table": "speed_limit_analysis", "group_by": "Speed_Limit", "db_column": "speed_limit"},
    {"table": "driver_fatigue_analysis", "group_by": "Driver_Fatigue", "db_column": "driver_fatigue"},
    {"table": "vehicle_condition_analysis", "group_by": "Vehicle_Condition", "db_column": "vehicle_condition"},
    {"table": "road_condition_analysis", "group_by": "Road_Condition", "db_column": "road_condition"},
    {"table": "weather_conditions_analysis", "group_by": "Weather_Conditions", "db_column": "weather_condition"},
    {"table": "vehicles_most_involved", "group_by": "Vehicle_Type", "db_column": "vehicle_type"}
]
def make_writer(table, group_col, db_col):
    def write(batch_df, batch_id):
        # last_batch_time["wide"] = time.time()
        rows = batch_df.collect()
        if not rows:
            return
        try:
            conn = pymysql.connect(host="localhost", user="admin", password="1234", database="traffic_analysis")
            data = []
            for r in rows:
                value = r[group_col]
                data.append((value, int(r['accident_count'])))
            query = f"""
            INSERT INTO {table} ({db_col}, accident_count, batch_time)
            VALUES (%s, %s, NOW())
            ON DUPLICATE KEY UPDATE
                accident_count = VALUES(accident_count),
                batch_time = VALUES(batch_time)
            """
            with conn.cursor() as cursor:
                cursor.executemany(query, data)
                conn.commit()
            conn.close()
        except Exception as e:
            print(f"Error in {table}:", e)
    return write

wide_queries = []
for cfg in aggregation_configs:
    agg = df_parsed.groupBy(cfg["group_by"]).agg(count("*").alias("accident_count"))
    cp = f"C:/BigDataProject/checkpoints/{cfg['table']}"
    os.makedirs(cp, exist_ok=True)
    q = agg.writeStream \
        .outputMode("update") \
        .foreachBatch(make_writer(cfg["table"], cfg["group_by"], cfg["db_column"])) \
        .option("checkpointLocation", cp) \
        .trigger(processingTime="30 seconds") \
        .start()
    wide_queries.append(q)
try:
    spark.streams.awaitAnyTermination()
except KeyboardInterrupt:
    pass
finally:
    narrow_query.stop()
    for q in wide_queries:
        q.stop()
    spark.stop()
