# -*- coding: utf-8 -*-
from __future__ import print_function
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, count, avg, lit, unix_timestamp
from pyspark.ml.feature import VectorAssembler, StringIndexer, OneHotEncoder
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml import Pipeline
import sys

# Инициализация Spark
spark = SparkSession.builder \
    .appName("DroneSwarmAnalysis") \
    .config("spark.sql.warehouse.dir", "hdfs://namenode:9000/user/hive/warehouse") \
    .config("hive.metastore.uris", "thrift://hive-metastore:9083") \
    .enableHiveSupport() \
    .getOrCreate()

print(">>> Loading drone events from Hive...")
# Читаем все события из Hive
df = spark.sql("SELECT * FROM drone_db.events")

# Удаляем строки с пропусками
df = df.na.drop()

print(">>> Original schema:")
df.printSchema()
print(">>> Sample data:")
df.show(5)

# 1. FEATURE ENGINEERING

# Преобразуем event_type в числовой признак (label для классификации)
indexer_event = StringIndexer(inputCol="event_type", outputCol="label")
df_indexed = indexer_event.fit(df).transform(df)

# Добавим категориальные признаки: state, mode
indexer_state = StringIndexer(inputCol="state", outputCol="state_index")
df_indexed = indexer_state.fit(df_indexed).transform(df_indexed)

indexer_mode = StringIndexer(inputCol="mode", outputCol="mode_index")
df_indexed = indexer_mode.fit(df_indexed).transform(df_indexed)

# Кодируем категориальные переменные (One-Hot не обязателен для деревьев, но можно)
# encoder = OneHotEncoder(inputCols=["state_index", "mode_index"], outputCols=["state_vec", "mode_vec"])

# Собираем числовые признаки
assembler = VectorAssembler(
    inputCols=[
        "drone_id",
        "zone_id",
        "x",
        "y",
        "battery",
        "state_index",
        "mode_index",
        "mission_time"
    ],
    outputCol="features"
)
data = assembler.transform(df_indexed)

# Разделим данные
train_data, test_data = data.randomSplit([0.8, 0.2], seed=42)

# 2. МОДЕЛЬ: Random Forest (лучше для категориальных данных и интерпретируемости)
print(">>> Training Random Forest classifier to predict event_type...")
rf = RandomForestClassifier(
    labelCol="label",
    featuresCol="features",
    numTrees=50,
    maxDepth=10,
    seed=42
)

model = rf.fit(train_data)

# 3. ОЦЕНКА ТОЧНОСТИ
result = model.transform(test_data)
evaluator = MulticlassClassificationEvaluator(
    labelCol="label",
    predictionCol="prediction",
    metricName="accuracy"
)
accuracy = evaluator.evaluate(result)
print(f">>> Model Accuracy (predicting event_type): {accuracy:.4f}")

# Покажем соответствие меток
label_mapping = indexer_event.fit(df).labels
print(">>> Label mapping (index -> event_type):")
for idx, label in enumerate(label_mapping):
    print(f"  {idx} -> {label}")

# Примеры предсказаний
print(">>> Prediction examples:")
result.select(
    "event_type", "prediction", "drone_id", "zone_id", "battery", "state"
).show(10)

# 4. ДОПОЛНИТЕЛЬНЫЙ АНАЛИЗ: Время обработки зоны
print("\n>>> Analyzing zone processing time...")

# Найдём время между 'zone_claimed' и 'zone_processed'
claimed = df.filter(col("event_type") == "zone_claimed").select(
    "zone_id", "timestamp", "drone_id"
).withColumnRenamed("timestamp", "claimed_at")

processed = df.filter(col("event_type") == "zone_processed").select(
    "zone_id", "timestamp"
).withColumnRenamed("timestamp", "processed_at")

processing_time = claimed.join(processed, "zone_id") \
    .withColumn("processing_duration_sec", col("processed_at") - col("claimed_at")) \
    .filter(col("processing_duration_sec") > 0)

avg_time = processing_time.agg(avg("processing_duration_sec")).collect()[0][0]
print(f"Average zone processing time: {avg_time:.2f} seconds")

# Сохраняем модель (опционально)
# model.write().overwrite().save("/tmp/drone_event_classifier")

spark.stop()
print(">>> Analysis completed successfully!")
