from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, regexp_replace, to_timestamp, to_date, sum as _sum, count, desc
)

# ================================
# 1. Crear sesión de Spark
# ================================
spark = SparkSession.builder \
    .appName("ProcesamientoBatchCoffeSales") \
    .getOrCreate()

print("=== Sesión de Spark inicializada ===")

# Ruta en HDFS
file_path = "hdfs://localhost:9000/Tarea3_procesamiento_batch/Coffe_sales.csv"

print(f"=== 1. Cargando datos desde: {file_path} ===")

df = spark.read.csv(file_path, header=True, inferSchema=True)

print("Columnas del dataset cargadas:")
print(df.columns)

# ================================
# 2. TRANSFORMACIONES
# ================================
print("=== 2. Realizando Transformaciones ===")

# Limpieza de columna money → quitar símbolos y convertir a float
df = df.withColumn("money_clean",
                   regexp_replace(col("money"), "[^0-9.]", "").cast("double"))

# Convertir Date a formato fecha
df = df.withColumn("sale_date", to_date(col("Date"), "M/d/yyyy"))

# Extraer año y mes
df = df.withColumn("sale_year", col("sale_date").substr(1, 4)) \
       .withColumn("sale_month", col("Monthsort"))

# ================================
# 3. ANÁLISIS BATCH
# ================================
print("=== 3. Análisis Batch ===")

# --- KPI 1: Ventas Totales Históricas ---
total_sales = df.select(_sum("money_clean")).collect()[0][0]
print(f"\nVentas Totales Históricas: ${total_sales if total_sales else 0}")

# --- KPI 2: Ventas mensuales + transacciones ---
ventas_mensuales = df.groupBy("sale_year", "sale_month", "Month_name") \
    .agg(
        _sum("money_clean").alias("monthly_sales"),
        count("*").alias("total_transactions")
    ).orderBy("sale_year", "sale_month")

print("\nVentas Mensuales y Transacciones:")
ventas_mensuales.show()

# --- KPI 3: Top 5 Productos por Ventas ---
top_productos = df.groupBy("coffee_name") \
    .agg(
        _sum("money_clean").alias("total_sales"),
        count("*").alias("transactions")
    ).orderBy(desc("total_sales")) \
    .limit(5)

print("\nTop 5 Productos por Ventas:")
top_productos.show()

# --- KPI 4: Mejores Horas del Día ---
horas = df.groupBy("hour_of_day") \
    .agg(
        _sum("money_clean").alias("total_sales"),
        count("*").alias("transactions")
    ).orderBy(desc("total_sales"))

print("\nMejores Horas del Día para Ventas:")
horas.show()

print("\n=== 4. Procesamiento Batch Completado ===")

spark.stop()
print("=== Sesión de Spark detenida ===")