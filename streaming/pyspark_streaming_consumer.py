# pyspark_streaming_consumer.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, StringType
import json

# ============================
# 1. DEFINIMOS EL ESQUEMA
# ============================
schema = StructType([
    StructField("hour_of_day", IntegerType()),
    StructField("cash_type", StringType()),
    StructField("money", FloatType()),
    StructField("coffee_name", StringType()),
    StructField("Time_of_Day", StringType()),
    StructField("Weekday", StringType()),
    StructField("Month_name", StringType()),
    StructField("Date", StringType()),
    StructField("Time", StringType())
])

# ============================
# 2. CREAR LA SESIÓN DE SPARK
# ============================
spark = (
    SparkSession.builder
        .appName("CoffeeSalesStreamingConsumer")
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1")
        .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")
print("\n=== Spark Streaming Consumer Iniciado ===\n")

# ============================
# 3. LEER STREAM DESDE KAFKA
# ============================
df_raw = (
    spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", "coffee_sales_stream")
        .option("startingOffsets", "latest")
        .load()
)

# Kafka entrega value como binario -> lo convertimos a string
df_string = df_raw.select(col("value").cast("string"))

# ============================
# 4. PARSEAR JSON
# ============================
df = df_string.select(from_json(col("value"), schema).alias("data")).select("data.*")

# ============================
# 5. ANÁLISIS EN VENTANAS DE 1 MINUTO
# ============================

# Agregar una columna timestamp sintética
from pyspark.sql.functions import current_timestamp
df = df.withColumn("event_time", current_timestamp())

# A. Ventas por minuto
sales_window = (
    df.groupBy(window(col("event_time"), "1 minute"))
      .agg({"money": "sum"})
      .withColumnRenamed("sum(money)", "total_sales")
)

# B. Transacciones por minuto
transactions_window = (
    df.groupBy(window(col("event_time"), "1 minute"))
      .agg({"hour_of_day": "count"})
      .withColumnRenamed("count(hour_of_day)", "total_transactions")
)

# C. Ventas por producto
products_window = (
    df.groupBy(window(col("event_time"), "1 minute"), "coffee_name")
      .agg({"money": "sum"})
      .withColumnRenamed("sum(money)", "total_sales")
)

# D. Ventas por hora del día
hours_window = (
    df.groupBy(window(col("event_time"), "1 minute"), "hour_of_day")
      .agg({"money": "sum"})
      .withColumnRenamed("sum(money)", "total_sales")
)

# ============================
# 6. ESCRIBIR RESULTADOS A CONSOLA
# ============================

query = (
    sales_window.writeStream
        .outputMode("update")
        .format("console")
        .option("truncate", False)
        .start()
)

query2 = (
    transactions_window.writeStream
        .outputMode("update")
        .format("console")
        .option("truncate", False)
        .start()
)

query3 = (
    products_window.writeStream
        .outputMode("update")
        .format("console")
        .option("truncate", False)
        .start()
)

query4 = (
    hours_window.writeStream
        .outputMode("update")
        .format("console")
        .option("truncate", False)
        .start()
)

# Esperar indefinidamente
query.awaitTermination()
query2.awaitTermination()
query3.awaitTermination()
query4.awaitTermination()