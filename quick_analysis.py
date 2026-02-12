#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Швидкий аналіз даних IMDb (оптимізована версія з вибіркою)
"""

import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, sum as spark_sum, avg, min as spark_min, max as spark_max,
    stddev, isnan, when, lit, countDistinct, split, size, length, trim
)
from data_loader import load_title_basics, load_title_ratings

# Налаштування
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# Spark сесія
spark = SparkSession.builder \
    .appName("IMDb Quick Analysis") \
    .master("local[*]") \
    .config("spark.driver.memory", "4g") \
    .config("spark.sql.shuffle.partitions", "8") \
    .config("spark.driver.host", "127.0.0.1") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .config("spark.sql.execution.arrow.pyspark.enabled", "false") \
    .getOrCreate()

print("=" * 80)
print("ШВИДКИЙ АНАЛІЗ ДАНИХ IMDb")
print("=" * 80)

# Завантаження даних
print("\nЗавантаження даних...")
df_titles = load_title_basics(spark)
df_ratings = load_title_ratings(spark)

# Фільтруємо тільки фільми з рейтингами для швидшого аналізу
print("\nФокус на фільмах з рейтингами для швидшого аналізу...")
df = df_titles.join(df_ratings, "tconst", "inner") \
    .filter(col("titleType") == "movie")

print(f"Відібрано {df.count():,} фільмів з рейтингами")

# ============================================================================
# ЗАВДАННЯ 1-2: ЗАГАЛЬНА ТА ЧИСЛОВА СТАТИСТИКА
# ============================================================================

print("\n" + "=" * 80)
print("ЗАВДАННЯ 1-2: СТАТИСТИКА")
print("=" * 80)

print("\nОписова статистика:")
df.select('startYear', 'runtimeMinutes', 'averageRating', 'numVotes').describe().show()

print("\nРозподіл по рокам (топ-15):")
df.groupBy("startYear").count().orderBy("startYear", ascending=False).show(15)

print("\nРозподіл по жанрах (топ-15):")
df.filter(col("genres").isNotNull()) \
    .groupBy("genres").count() \
    .orderBy("count", ascending=False).show(15, truncate=50)

# ============================================================================
# ЗАВДАННЯ 3: ОБРОБКА ТИПІВ
# ============================================================================

print("\n" + "=" * 80)
print("ЗАВДАННЯ 3: ОБРОБКА ТИПІВ")
print("=" * 80)

# Парсинг жанрів
df = df.withColumn("genres_array", split(col("genres"), ",")) \
       .withColumn("genres_count", size(col("genres_array")))

# Категоризація
df = df.withColumn("duration_category",
    when(col("runtimeMinutes") < 60, "Short")
    .when((col("runtimeMinutes") >= 60) & (col("runtimeMinutes") < 120), "Standard")
    .when(col("runtimeMinutes") >= 120, "Long")
    .otherwise("Unknown"))

df = df.withColumn("rating_category",
    when(col("averageRating") < 6.0, "Below Average")
    .when((col("averageRating") >= 6.0) & (col("averageRating") < 7.0), "Average")
    .when((col("averageRating") >= 7.0) & (col("averageRating") < 8.0), "Good")
    .when(col("averageRating") >= 8.0, "Excellent")
    .otherwise("Unknown"))

df = df.withColumn("decade", (col("startYear") / 10).cast("int") * 10)

print("\nКатегорії тривалості:")
df.groupBy("duration_category").count().show()

print("\nКатегорії рейтингів:")
df.groupBy("rating_category").count().show()

print("\nРозподіл по декадам:")
df.groupBy("decade").count().orderBy("decade", ascending=False).show(15)

# ============================================================================
# ЗАВДАННЯ 4: ІНФОРМАТИВНІСТЬ ОЗНАК
# ============================================================================

print("\n" + "=" * 80)
print("ЗАВДАННЯ 4: ІНФОРМАТИВНІСТЬ ОЗНАК")
print("=" * 80)

print("\nАналіз унікальності:")
for col_name in ['tconst', 'primaryTitle', 'startYear', 'genres', 
                 'runtimeMinutes', 'averageRating']:
    distinct = df.select(countDistinct(col_name)).collect()[0][0]
    total = df.count()
    null_cnt = df.filter(col(col_name).isNull()).count()
    print(f"{col_name:20} | Унікальних: {distinct:8,} | "
          f"Null: {null_cnt:6,} | Унікальність: {distinct*100.0/total:5.2f}%")

print("\nВплив жанрів на рейтинг:")
from pyspark.sql.functions import explode
df.filter(col("genres").isNotNull()) \
    .select(explode(col("genres_array")).alias("genre"), "averageRating", "numVotes") \
    .groupBy("genre") \
    .agg(count("*").alias("count"),
         avg("averageRating").alias("avg_rating"),
         avg("numVotes").alias("avg_votes")) \
    .filter(col("count") >= 1000) \
    .orderBy("avg_rating", ascending=False).show(15, truncate=False)

print("\nВплив декади на рейтинг:")
df.groupBy("decade") \
    .agg(count("*").alias("count"),
         avg("averageRating").alias("avg_rating")) \
    .orderBy("decade", ascending=False).show(15)

# ============================================================================
# ЗАВДАННЯ 5: ПРОПУЩЕНІ ЗНАЧЕННЯ ТА ДУБЛІКАТИ
# ============================================================================

print("\n" + "=" * 80)
print("ЗАВДАННЯ 5: ПРОПУЩЕНІ ЗНАЧЕННЯ ТА ДУБЛІКАТИ")
print("=" * 80)

total = df.count()
print(f"\nЗагальна кількість записів: {total:,}\n")

print("Аналіз пропущених значень:")
for col_name in ['tconst', 'primaryTitle', 'startYear', 'runtimeMinutes', 
                 'genres', 'averageRating', 'numVotes']:
    null_cnt = df.filter(col(col_name).isNull()).count()
    print(f"{col_name:20} | Null: {null_cnt:8,} ({null_cnt*100.0/total:5.2f}%)")

print("\nАналіз дублікатів по tconst:")
dup_tconst = df.groupBy("tconst").count().filter(col("count") > 1).count()
print(f"Дублікатів: {dup_tconst}")

print("\nАналіз дублікатів по назві+рік:")
dup_title_year = df.groupBy("primaryTitle", "startYear") \
    .count().filter(col("count") > 1) \
    .orderBy("count", ascending=False)
print(f"Груп дублікатів: {dup_title_year.count():,}")
if dup_title_year.count() > 0:
    print("\nТоп-10:")
    dup_title_year.show(10, truncate=50)

# Очищення даних
print("\n" + "=" * 80)
print("ОЧИЩЕННЯ ДАНИХ")
print("=" * 80)

df_clean = df

# Видалення дорослого контенту
before = df_clean.count()
df_clean = df_clean.filter(col("isAdult") == 0)
after = df_clean.count()
print(f"\n1. Видалено дорослий контент: {before - after:,} записів")

# Фільтрація аномальних значень runtimeMinutes
before = df_clean.count()
df_clean = df_clean.filter(
    (col("runtimeMinutes").isNotNull()) &
    (col("runtimeMinutes") > 0) &
    (col("runtimeMinutes") < 500)  # Розумна межа для фільмів
)
after = df_clean.count()
print(f"2. Видалено аномальні тривалості: {before - after:,} записів")

# Фільтрація майбутніх років
before = df_clean.count()
df_clean = df_clean.filter(col("startYear") <= 2026)
after = df_clean.count()
print(f"3. Видалено майбутні дати: {before - after:,} записів")

# Заповнення genres
before_null = df_clean.filter(col("genres").isNull()).count()
df_clean = df_clean.withColumn("genres",
    when(col("genres").isNull(), "Unknown").otherwise(col("genres")))
after_null = df_clean.filter(col("genres").isNull()).count()
print(f"4. Заповнено genres: {before_null - after_null:,} записів")

print(f"\nВихідно: {total:,} записів")
print(f"Очищено: {df_clean.count():,} записів")
print(f"Видалено: {total - df_clean.count():,} записів "
      f"({(total - df_clean.count())*100.0/total:.2f}%)")

# Топ фільми після очищення
print("\n" + "=" * 80)
print("ТОП-20 ФІЛЬМІВ ПІСЛЯ ОЧИЩЕННЯ")
print("=" * 80)

df_clean.filter(col("numVotes") >= 50000) \
    .orderBy(col("averageRating").desc(), col("numVotes").desc()) \
    .select("primaryTitle", "startYear", "averageRating", "numVotes", "genres") \
    .show(20, truncate=50)

# Підсумок
print("\n" + "=" * 80)
print("ПІДСУМКОВИЙ ЗВІТ")
print("=" * 80)
print(f"""
АНАЛІЗ ЗАВЕРШЕНО УСПІШНО!

СТАТИСТИКА:
- Всього фільмів з рейтингами: {total:,}
- Після очищення: {df_clean.count():,}
- Середній рейтинг: {df_clean.agg(avg("averageRating")).collect()[0][0]:.2f}
- Середня кількість голосів: {df_clean.agg(avg("numVotes")).collect()[0][0]:.0f}

ВИКОНАНО:
✓ Завдання 1: Загальна статистика
✓ Завдання 2: Аналіз числових ознак
✓ Завдання 3: Обробка типів та парсинг
✓ Завдання 4: Аналіз інформативності
✓ Завдання 5: Обробка пропусків та дублікатів

Очищені дані готові до подальшого аналізу та моделювання.
""")

spark.stop()
print("\nSpark сесію закрито.")
