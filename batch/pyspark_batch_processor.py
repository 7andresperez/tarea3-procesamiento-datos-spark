from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, regexp_replace, to_timestamp, to_date,
    sum as _sum, count, desc
)
import os

# ================================
# 1. Crear sesión de Spark
# ================================
spark = SparkSession.builder \
    .appName("ProcesamientoBatchCoffeSales") \
    .getOrCreate()

print("=== Sesión de Spark inicializada ===")

# Ruta de entrada en HDFS
file_path = "hdfs://localhost:9000/Tarea3_procesamiento_batch/Coffe_sales.csv"

print(f"=== 1. Cargando datos desde: {file_path} ===")

df = spark.read.csv(file_path, header=True, inferSchema=True)

print("Columnas del dataset cargadas:")
print(df.columns)

# Crear carpeta de salida local
output_dir = "/home/hadoop/coffee_batch_results"
os.makedirs(output_dir, exist_ok=True)
print(f"Los resultados serán almacenados en: {output_dir}")

# ================================
# 2. TRANSFORMACIONES
# ================================
print("=== 2. Realizando Transformaciones ===")

df = df.withColumn("money_clean",
                   regexp_replace(col("money"), "[^0-9.]", "").cast("double"))

df = df.withColumn("sale_date", to_date(col("Date"), "M/d/yyyy"))

df = df.withColumn("sale_year", col("sale_date").substr(1, 4)) \
       .withColumn("sale_month", col("Monthsort"))

# ================================
# 3. ANÁLISIS BATCH
# ================================
print("=== 3. Análisis Batch ===")

# --- KPI 1: Ventas Totales Históricas ---
total_sales = df.select(_sum("money_clean")).collect()[0][0]
print(f"\nVentas Totales Históricas: ${total_sales if total_sales else 0}")

# Guardar KPI total en CSV
spark.createDataFrame(
    [(float(total_sales),)],
    ["total_sales"]
).write.mode("overwrite").csv(f"{output_dir}/total_sales_kpi")


# --- KPI 2: Ventas mensuales + transacciones ---
ventas_mensuales = df.groupBy("sale_year", "sale_month", "Month_name") \
    .agg(
        _sum("money_clean").alias("monthly_sales"),
        count("*").alias("total_transactions")
    ).orderBy("sale_year", "sale_month")

print("\nVentas Mensuales y Transacciones:")
ventas_mensuales.show()

ventas_mensuales.write.mode("overwrite").csv(f"{output_dir}/ventas_mensuales")


# --- KPI 3: Top 5 Productos por Ventas ---
top_productos = df.groupBy("coffee_name") \
    .agg(
        _sum("money_clean").alias("total_sales"),
        count("*").alias("transactions")
    ).orderBy(desc("total_sales")) \
    .limit(5)

print("\nTop 5 Productos por Ventas:")
top_productos.show()

top_productos.write.mode("overwrite").csv(f"{output_dir}/top_5_productos")


# --- KPI 4: Mejores Horas del Día ---
horas = df.groupBy("hour_of_day") \
    .agg(
        _sum("money_clean").alias("total_sales"),
        count("*").alias("transactions")
    ).orderBy(desc("total_sales"))

print("\nMejores Horas del Día para Ventas:")
horas.show()

horas.write.mode("overwrite").csv(f"{output_dir}/mejores_horas")


# ================================
# Fin
# ================================
print("\n=== 4. Procesamiento Batch Completado ===")
print(f"Resultados exportados a: {output_dir}")

spark.stop()
print("=== Sesión de Spark detenida ===")