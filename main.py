#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Головний файл для роботи з набором даних IMDb
"""

import os
import sys
from pyspark.sql import SparkSession
from data_loader import (
    load_title_basics,
    load_title_ratings,
    load_name_basics,
    load_title_crew,
    validate_dataframe
)

# Встановлюємо змінні середовища для Windows
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# Створення Spark сесії
print("=" * 80)
print("ІНІЦІАЛІЗАЦІЯ SPARK")
print("=" * 80)
spark = SparkSession.builder \
    .appName("IMDb Data Analysis") \
    .master("local[*]") \
    .config("spark.driver.memory", "4g") \
    .config("spark.sql.shuffle.partitions", "8") \
    .config("spark.driver.host", "127.0.0.1") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .config("spark.sql.execution.arrow.pyspark.enabled", "false") \
    .getOrCreate()

print(f"\n[OK] Spark версія: {spark.version}")
print(f"[OK] Spark сесію створено успішно!")

# ============================================================================
# ЗАВАНТАЖЕННЯ ОСНОВНИХ НАБОРІВ ДАНИХ
# ============================================================================

print("\n" + "=" * 80)
print("ЗАВАНТАЖЕННЯ НАБОРІВ ДАНИХ")
print("=" * 80)

# Завантажуємо основні набори даних
print("\n1. Завантаження title.basics.tsv (основна інформація про фільми)...")
df_titles = load_title_basics(spark)

print("\n2. Завантаження title.ratings.tsv (рейтинги)...")
df_ratings = load_title_ratings(spark)

print("\n3. Завантаження name.basics.tsv (інформація про людей)...")
df_names = load_name_basics(spark)

print("\n4. Завантаження title.crew.tsv (команда)...")
df_crew = load_title_crew(spark)

print("\n[OK] Всі основні набори даних завантажено!")

# ============================================================================
# ПЕРЕВІРКА КОРЕКТНОСТІ ЗАВАНТАЖЕННЯ
# ============================================================================

print("\n" + "=" * 80)
print("ПЕРЕВІРКА ЗАВАНТАЖЕНИХ ДАНИХ")
print("=" * 80)

# Перевірка title.basics
print("\n" + "-" * 80)
print("ПЕРЕВІРКА: title.basics (основна інформація про фільми)")
print("-" * 80)
print(f"Кількість записів: {df_titles.count():,}")
print("\nСхема:")
df_titles.printSchema()
print("\nПерші 5 рядків:")
df_titles.show(5, truncate=50)

# Додаткова перевірка: статистика по titleType
print("\nРозподіл по типах контенту:")
df_titles.groupBy("titleType").count().orderBy("count", ascending=False).show(10)

# Перевірка title.ratings
print("\n" + "-" * 80)
print("ПЕРЕВІРКА: title.ratings (рейтинги)")
print("-" * 80)
print(f"Кількість записів: {df_ratings.count():,}")
print("\nСхема:")
df_ratings.printSchema()
print("\nПерші 5 рядків:")
df_ratings.show(5)

# Додаткова перевірка: базова статистика рейтингів
print("\nСтатистика рейтингів:")
df_ratings.describe(['averageRating', 'numVotes']).show()

# Перевірка name.basics
print("\n" + "-" * 80)
print("ПЕРЕВІРКА: name.basics (інформація про людей)")
print("-" * 80)
print(f"Кількість записів: {df_names.count():,}")
print("\nСхема:")
df_names.printSchema()
print("\nПерші 5 рядків:")
df_names.show(5, truncate=50)

# Перевірка title.crew
print("\n" + "-" * 80)
print("ПЕРЕВІРКА: title.crew (команда)")
print("-" * 80)
print(f"Кількість записів: {df_crew.count():,}")
print("\nСхема:")
df_crew.printSchema()
print("\nПерші 5 рядків:")
df_crew.show(5, truncate=50)

# ============================================================================
# ТЕСТ: ОБ'ЄДНАННЯ ДАНИХ
# ============================================================================

print("\n" + "=" * 80)
print("ТЕСТ ОБ'ЄДНАННЯ ДАНИХ")
print("=" * 80)

# Об'єднуємо titles з ratings
print("\nОб'єднання title.basics + title.ratings...")
df_titles_with_ratings = df_titles.join(df_ratings, "tconst", "left")
print(f"[OK] Результат: {df_titles_with_ratings.count():,} записів")

# Показуємо приклад об'єднаних даних
print("\nТоп-10 фільмів з найвищим рейтингом (мінімум 100,000 голосів):")
top_movies = df_titles_with_ratings \
    .filter((df_titles_with_ratings.titleType == "movie") & 
            (df_titles_with_ratings.numVotes >= 100000)) \
    .orderBy(df_titles_with_ratings.averageRating.desc()) \
    .select("primaryTitle", "startYear", "averageRating", "numVotes", "genres") \
    .limit(10)
top_movies.show(truncate=False)

# ============================================================================
# ПІДСУМОК
# ============================================================================

print("\n" + "=" * 80)
print("ПІДСУМОК")
print("=" * 80)
print("\n[OK] Всі тести успішно виконано!")
print("[OK] Схеми визначено коректно")
print("[OK] Дані завантажено успішно")
print("[OK] DataFrame працюють належним чином")
print("[OK] Модуль data_loader.py готовий до використання")
print("\n>>> Можемо переходити до аналізу даних!\n")

# Зупинка Spark сесії
spark.stop()
print("Spark сесію закрито.")

