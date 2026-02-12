#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Модуль для завантаження та обробки даних IMDb
Містить схеми та функції для зчитування TSV файлів
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, 
    DoubleType, BooleanType
)
from pyspark.sql.functions import col, sum as spark_sum
import os


# ============================================================================
# ВИЗНАЧЕННЯ СХЕМ ДЛЯ КОЖНОГО НАБОРУ ДАНИХ
# ============================================================================

def get_name_basics_schema():
    """
    Схема для name.basics.tsv - інформація про людей
    
    Поля:
    - nconst: унікальний ідентифікатор особи (string)
    - primaryName: ім'я (string)
    - birthYear: рік народження (int)
    - deathYear: рік смерті (int)
    - primaryProfession: основні професії (string, comma-separated)
    - knownForTitles: відомі роботи (string, comma-separated)
    """
    return StructType([
        StructField("nconst", StringType(), True),
        StructField("primaryName", StringType(), True),
        StructField("birthYear", IntegerType(), True),
        StructField("deathYear", IntegerType(), True),
        StructField("primaryProfession", StringType(), True),
        StructField("knownForTitles", StringType(), True)
    ])


def get_title_basics_schema():
    """
    Схема для title.basics.tsv - основна інформація про фільми/серіали
    
    Поля:
    - tconst: унікальний ідентифікатор title (string)
    - titleType: тип (movie, tvSeries, short, etc.)
    - primaryTitle: основна назва (string)
    - originalTitle: оригінальна назва (string)
    - isAdult: дорослий контент (boolean: 0=false, 1=true)
    - startYear: рік випуску (int)
    - endYear: рік закінчення для серіалів (int)
    - runtimeMinutes: тривалість у хвилинах (int)
    - genres: жанри (string, comma-separated)
    """
    return StructType([
        StructField("tconst", StringType(), True),
        StructField("titleType", StringType(), True),
        StructField("primaryTitle", StringType(), True),
        StructField("originalTitle", StringType(), True),
        StructField("isAdult", IntegerType(), True),
        StructField("startYear", IntegerType(), True),
        StructField("endYear", IntegerType(), True),
        StructField("runtimeMinutes", IntegerType(), True),
        StructField("genres", StringType(), True)
    ])


def get_title_ratings_schema():
    """
    Схема для title.ratings.tsv - рейтинги
    
    Поля:
    - tconst: ідентифікатор title (string)
    - averageRating: середній рейтинг (double, 1-10)
    - numVotes: кількість голосів (int)
    """
    return StructType([
        StructField("tconst", StringType(), True),
        StructField("averageRating", DoubleType(), True),
        StructField("numVotes", IntegerType(), True)
    ])


def get_title_crew_schema():
    """
    Схема для title.crew.tsv - режисери та сценаристи
    
    Поля:
    - tconst: ідентифікатор title (string)
    - directors: список nconst режисерів (string, comma-separated)
    - writers: список nconst сценаристів (string, comma-separated)
    """
    return StructType([
        StructField("tconst", StringType(), True),
        StructField("directors", StringType(), True),
        StructField("writers", StringType(), True)
    ])


def get_title_principals_schema():
    """
    Схема для title.principals.tsv - головні учасники
    
    Поля:
    - tconst: ідентифікатор title (string)
    - ordering: порядок в титрах (int)
    - nconst: ідентифікатор особи (string)
    - category: категорія (actor, director, writer, etc.)
    - job: конкретна робота (string, може бути null)
    - characters: персонажі (string, JSON format)
    """
    return StructType([
        StructField("tconst", StringType(), True),
        StructField("ordering", IntegerType(), True),
        StructField("nconst", StringType(), True),
        StructField("category", StringType(), True),
        StructField("job", StringType(), True),
        StructField("characters", StringType(), True)
    ])


def get_title_akas_schema():
    """
    Схема для title.akas.tsv - альтернативні назви
    
    Поля:
    - titleId: ідентифікатор title (string)
    - ordering: порядковий номер (int)
    - title: локалізована назва (string)
    - region: регіон/країна (string)
    - language: мова (string)
    - types: типи (string)
    - attributes: додаткові атрибути (string)
    - isOriginalTitle: чи є оригінальною назвою (boolean)
    """
    return StructType([
        StructField("titleId", StringType(), True),
        StructField("ordering", IntegerType(), True),
        StructField("title", StringType(), True),
        StructField("region", StringType(), True),
        StructField("language", StringType(), True),
        StructField("types", StringType(), True),
        StructField("attributes", StringType(), True),
        StructField("isOriginalTitle", IntegerType(), True)
    ])


def get_title_episode_schema():
    """
    Схема для title.episode.tsv - епізоди серіалів
    
    Поля:
    - tconst: ідентифікатор епізоду (string)
    - parentTconst: ідентифікатор серіалу (string)
    - seasonNumber: номер сезону (int)
    - episodeNumber: номер епізоду (int)
    """
    return StructType([
        StructField("tconst", StringType(), True),
        StructField("parentTconst", StringType(), True),
        StructField("seasonNumber", IntegerType(), True),
        StructField("episodeNumber", IntegerType(), True)
    ])


# ============================================================================
# ФУНКЦІЇ ДЛЯ ЗАВАНТАЖЕННЯ ДАНИХ
# ============================================================================

def load_tsv_with_schema(spark: SparkSession, file_path: str, schema: StructType) -> DataFrame:
    """
    Завантажує TSV файл з вказаною схемою
    
    Args:
        spark: SparkSession об'єкт
        file_path: шлях до TSV файлу
        schema: схема даних (StructType)
    
    Returns:
        DataFrame з завантаженими даними
    """
    return spark.read.csv(
        file_path,
        sep='\t',
        header=True,
        schema=schema,
        nullValue='\\N',  # IMDb використовує '\\N' для null значень
        quote='',  # Вимкнути обробку лапок
        escape=''  # Вимкнути escape символи
    )


def load_name_basics(spark: SparkSession, data_dir: str = "dataset") -> DataFrame:
    """
    Завантажує дані про людей (name.basics.tsv)
    
    Args:
        spark: SparkSession об'єкт
        data_dir: директорія з даними
    
    Returns:
        DataFrame з інформацією про людей
    """
    file_path = os.path.join(data_dir, "name.basics.tsv")
    schema = get_name_basics_schema()
    return load_tsv_with_schema(spark, file_path, schema)


def load_title_basics(spark: SparkSession, data_dir: str = "dataset") -> DataFrame:
    """
    Завантажує основну інформацію про фільми (title.basics.tsv)
    
    Args:
        spark: SparkSession об'єкт
        data_dir: директорія з даними
    
    Returns:
        DataFrame з основною інформацією про фільми
    """
    file_path = os.path.join(data_dir, "title.basics.tsv")
    schema = get_title_basics_schema()
    return load_tsv_with_schema(spark, file_path, schema)


def load_title_ratings(spark: SparkSession, data_dir: str = "dataset") -> DataFrame:
    """
    Завантажує рейтинги (title.ratings.tsv)
    
    Args:
        spark: SparkSession об'єкт
        data_dir: директорія з даними
    
    Returns:
        DataFrame з рейтингами
    """
    file_path = os.path.join(data_dir, "title.ratings.tsv")
    schema = get_title_ratings_schema()
    return load_tsv_with_schema(spark, file_path, schema)


def load_title_crew(spark: SparkSession, data_dir: str = "dataset") -> DataFrame:
    """
    Завантажує інформацію про команду (title.crew.tsv)
    
    Args:
        spark: SparkSession об'єкт
        data_dir: директорія з даними
    
    Returns:
        DataFrame з інформацією про команду
    """
    file_path = os.path.join(data_dir, "title.crew.tsv")
    schema = get_title_crew_schema()
    return load_tsv_with_schema(spark, file_path, schema)


def load_title_principals(spark: SparkSession, data_dir: str = "dataset") -> DataFrame:
    """
    Завантажує інформацію про головних учасників (title.principals.tsv)
    
    Args:
        spark: SparkSession об'єкт
        data_dir: директорія з даними
    
    Returns:
        DataFrame з інформацією про головних учасників
    """
    file_path = os.path.join(data_dir, "title.principals.tsv")
    schema = get_title_principals_schema()
    return load_tsv_with_schema(spark, file_path, schema)


def load_title_akas(spark: SparkSession, data_dir: str = "dataset") -> DataFrame:
    """
    Завантажує альтернативні назви (title.akas.tsv)
    
    Args:
        spark: SparkSession об'єкт
        data_dir: директорія з даними
    
    Returns:
        DataFrame з альтернативними назвами
    """
    file_path = os.path.join(data_dir, "title.akas.tsv")
    schema = get_title_akas_schema()
    return load_tsv_with_schema(spark, file_path, schema)


def load_title_episode(spark: SparkSession, data_dir: str = "dataset") -> DataFrame:
    """
    Завантажує інформацію про епізоди (title.episode.tsv)
    
    Args:
        spark: SparkSession об'єкт
        data_dir: директорія з даними
    
    Returns:
        DataFrame з інформацією про епізоди
    """
    file_path = os.path.join(data_dir, "title.episode.tsv")
    schema = get_title_episode_schema()
    return load_tsv_with_schema(spark, file_path, schema)


def load_all_datasets(spark: SparkSession, data_dir: str = "dataset") -> dict:
    """
    Завантажує всі набори даних IMDb
    
    Args:
        spark: SparkSession об'єкт
        data_dir: директорія з даними
    
    Returns:
        Словник з усіма DataFrame: {
            'name_basics': DataFrame,
            'title_basics': DataFrame,
            'title_ratings': DataFrame,
            'title_crew': DataFrame,
            'title_principals': DataFrame,
            'title_akas': DataFrame,
            'title_episode': DataFrame
        }
    """
    print("Завантаження наборів даних IMDb...")
    
    datasets = {}
    
    print("  Завантаження name.basics.tsv...")
    datasets['name_basics'] = load_name_basics(spark, data_dir)
    
    print("  Завантаження title.basics.tsv...")
    datasets['title_basics'] = load_title_basics(spark, data_dir)
    
    print("  Завантаження title.ratings.tsv...")
    datasets['title_ratings'] = load_title_ratings(spark, data_dir)
    
    print("  Завантаження title.crew.tsv...")
    datasets['title_crew'] = load_title_crew(spark, data_dir)
    
    print("  Завантаження title.principals.tsv...")
    datasets['title_principals'] = load_title_principals(spark, data_dir)
    
    print("  Завантаження title.akas.tsv...")
    datasets['title_akas'] = load_title_akas(spark, data_dir)
    
    print("  Завантаження title.episode.tsv...")
    datasets['title_episode'] = load_title_episode(spark, data_dir)
    
    print("Всі набори даних завантажено успішно!")
    
    return datasets


def validate_dataframe(df: DataFrame, df_name: str, show_samples: bool = True) -> None:
    """
    Перевіряє коректність завантаження DataFrame
    
    Args:
        df: DataFrame для перевірки
        df_name: назва DataFrame для виводу
        show_samples: чи показувати зразки даних
    """
    print(f"\n{'=' * 80}")
    print(f"ПЕРЕВІРКА: {df_name}")
    print('=' * 80)
    
    # Виводимо схему
    print("\nСхема:")
    df.printSchema()
    
    # Підраховуємо кількість рядків (це може зайняти час для великих файлів)
    print(f"\nЗагальна кількість рядків: {df.count():,}")
    
    # Показуємо перші рядки
    if show_samples:
        print("\nПерші 5 рядків:")
        df.show(5, truncate=True)
    
    # Перевіряємо наявність null значень
    print("\nСтатистика null значень:")
    null_counts = df.select([
        spark_sum(col(c).isNull().cast("int")).alias(c) 
        for c in df.columns
    ])
    null_counts.show(vertical=True)
    
    print(f"\n[OK] DataFrame '{df_name}' завантажено та перевірено успішно!")

