# Tarea 3 - Procesamiento de Datos con Apache Spark

**Resumen**
Pipeline académico que implementa procesamiento batch y streaming sobre un dataset de ventas de café. Integra Apache Spark (Batch y Structured Streaming), Apache Kafka y HDFS.

## Estructura
tarea3-procesamiento-datos-spark/
│
├── README.md
├── requirements.txt
│
├── batch/
│   ├── pyspark_batch_processor.py
│   └── config/
│       └── batch_config.yaml
│
├── streaming/
│   ├── kafka_producer_simulator.py
│   ├── pyspark_streaming_consumer.py
│   └── config/
│       └── streaming_config.yaml
│
├── scripts/
│   ├── upload_to_hdfs.sh
│   ├── run_batch.sh
│   └── run_streaming.sh
│
├── datasets/
│   └── Coffee_Sales.csv
│
├── docs/
│   ├── arquitectura.png
│   ├── flujo_batch.png
│   ├── flujo_streaming.png
│   └── demo_results.md
│
└── .gitignore

## Requisitos
- Ubuntu Server (VM)
- Java 11+ (para Hadoop/Spark)
- Hadoop HDFS en tu cluster
- Apache Kafka (broker) para la parte streaming
- Python 3.8 - 3.11 (evitar 3.13 con PySpark)


Instalación de dependencias (virtualenv recomendado):


```bash
python3.11 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
