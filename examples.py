#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Приклади використання модуля data_loader.py
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

# Налаштування для Windows
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# Створення Spark сесії
spark = SparkSession.builder \
    .appName("IMDb Examples") \
    .master("local[*]") \
    .config("spark.driver.memory", "4g") \
    .config("spark.sql.execution.arrow.pyspark.enabled", "false") \
    .getOrCreate()

print("=" * 80)
print("ПРИКЛАД 1: Завантаження одного набору даних")
print("=" * 80)

# Завантаження рейтингів
df_ratings = load_title_ratings(spark)
print(f"\nЗавантажено {df_ratings.count():,} рейтингів")
df_ratings.show(5)

print("\n" + "=" * 80)
print("ПРИКЛАД 2: Об'єднання кількох наборів даних")
print("=" * 80)

# Завантаження фільмів та рейтингів
df_titles = load_title_basics(spark)
df_ratings = load_title_ratings(spark)

# Об'єднання
df_movies_with_ratings = df_titles \
    .join(df_ratings, "tconst", "inner") \
    .filter(df_titles.titleType == "movie")

print(f"\nОб'єднано {df_movies_with_ratings.count():,} фільмів з рейтингами")

print("\n" + "=" * 80)
print("ПРИКЛАД 3: Аналіз найпопулярніших фільмів")
print("=" * 80)

# Топ фільми по кількості голосів
top_voted = df_movies_with_ratings \
    .orderBy(df_movies_with_ratings.numVotes.desc()) \
    .select("primaryTitle", "startYear", "averageRating", "numVotes") \
    .limit(10)

print("\nТоп-10 фільмів по кількості голосів:")
top_voted.show(truncate=False)

print("\n" + "=" * 80)
print("ПРИКЛАД 4: Аналіз по жанрах")
print("=" * 80)

# Розділення жанрів та підрахунок
from pyspark.sql.functions import explode, split

df_genres = df_movies_with_ratings \
    .filter(df_movies_with_ratings.genres.isNotNull()) \
    .select(
        "tconst",
        explode(split(df_movies_with_ratings.genres, ",")).alias("genre"),
        "averageRating",
        "numVotes"
    )

# Середній рейтинг по жанрах (мінімум 1000 фільмів)
genre_stats = df_genres \
    .groupBy("genre") \
    .agg(
        {"averageRating": "avg", "*": "count"}
    ) \
    .withColumnRenamed("avg(averageRating)", "avg_rating") \
    .withColumnRenamed("count(1)", "movie_count") \
    .filter("movie_count >= 1000") \
    .orderBy("avg_rating", ascending=False)

print("\nСередній рейтинг по жанрах:")
genre_stats.show(truncate=False)

print("\n" + "=" * 80)
print("ПРИКЛАД 5: Аналіз режисерів")
print("=" * 80)

# Завантаження команди
df_crew = load_title_crew(spark)

# Фільми з режисерами та рейтингами
df_directors = df_titles \
    .join(df_crew, "tconst") \
    .join(df_ratings, "tconst") \
    .filter((df_titles.titleType == "movie") & 
            (df_crew.directors.isNotNull()) & 
            (df_ratings.numVotes >= 1000))

print(f"\nЗнайдено {df_directors.count():,} фільмів з режисерами та рейтингами")
df_directors.select("primaryTitle", "directors", "startYear", "averageRating").show(5, truncate=50)

print("\n" + "=" * 80)
print("ПРИКЛАД 6: Фільтрація по рокам")
print("=" * 80)

# Фільми 2010-2024 років з високим рейтингом
modern_hits = df_movies_with_ratings \
    .filter((df_movies_with_ratings.startYear >= 2010) & 
            (df_movies_with_ratings.startYear <= 2024) &
            (df_movies_with_ratings.averageRating >= 8.0) &
            (df_movies_with_ratings.numVotes >= 50000)) \
    .orderBy(df_movies_with_ratings.averageRating.desc()) \
    .select("primaryTitle", "startYear", "averageRating", "numVotes", "genres") \
    .limit(20)

print("\nСучасні фільми (2010-2024) з рейтингом >= 8.0:")
modern_hits.show(truncate=False)

print("\n" + "=" * 80)
print("ПРИКЛАД 7: Використання валідації")
print("=" * 80)

# Валідація DataFrame
validate_dataframe(df_ratings, "title.ratings", show_samples=True)

# Закриття сесії
spark.stop()
print("\nСесію завершено.")
