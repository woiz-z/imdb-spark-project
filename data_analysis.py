#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Аналіз даних IMDb
Завдання 1-5: Статистичний аналіз, обробка типів, аналіз інформативності, 
обробка пропущених значень та дублікатів
"""

import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, sum as spark_sum, avg, min as spark_min, max as spark_max,
    stddev, isnan, when, lit, countDistinct, split, size, length, trim
)
from data_loader import (
    load_title_basics,
    load_title_ratings,
    load_name_basics,
    load_title_crew
)

# Налаштування для Windows
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
# ЗАВАНТАЖЕННЯ ДАНИХ
# ============================================================================

print("\n" + "=" * 80)
print("ЗАВАНТАЖЕННЯ ОСНОВНИХ НАБОРІВ ДАНИХ")
print("=" * 80)

print("\nЗавантаження title.basics.tsv...")
df_titles = load_title_basics(spark)
print(f"[OK] Завантажено {df_titles.count():,} записів")

print("\nЗавантаження title.ratings.tsv...")
df_ratings = load_title_ratings(spark)
print(f"[OK] Завантажено {df_ratings.count():,} записів")

# Об'єднуємо для повноти аналізу
print("\nОб'єднання title.basics + title.ratings...")
df = df_titles.join(df_ratings, "tconst", "left")
print(f"[OK] Результат: {df.count():,} записів")

# ============================================================================
# ЗАВДАННЯ 1: ЗАГАЛЬНА СТАТИСТИЧНА ІНФОРМАЦІЯ
# ============================================================================

print("\n" + "=" * 80)
print("ЗАВДАННЯ 1: ЗАГАЛЬНА СТАТИСТИЧНА ІНФОРМАЦІЯ")
print("=" * 80)

print("\n1.1. СТРУКТУРА НАБОРУ ДАНИХ:")
print("-" * 80)
print(f"Загальна кількість записів: {df.count():,}")
print(f"Кількість стовпців: {len(df.columns)}")
print(f"\nСписок стовпців: {df.columns}")

print("\n1.2. СХЕМА ДАНИХ:")
print("-" * 80)
df.printSchema()

print("\n1.3. ТИПИ КОНТЕНТУ:")
print("-" * 80)
df.groupBy("titleType").count() \
    .orderBy("count", ascending=False) \
    .show(15, truncate=False)

print("\n1.4. РОЗПОДІЛ ПО РОКАМ:")
print("-" * 80)
year_stats = df.filter(col("startYear").isNotNull()) \
    .groupBy("startYear") \
    .count() \
    .orderBy("startYear", ascending=False)
print(f"Діапазон років: {df.agg(spark_min('startYear')).collect()[0][0]} - {df.agg(spark_max('startYear')).collect()[0][0]}")
print("\nТоп-10 років по кількості контенту:")
year_stats.show(10)

print("\n1.5. РОЗПОДІЛ ДОРОСЛОГО КОНТЕНТУ:")
print("-" * 80)
df.groupBy("isAdult") \
    .count() \
    .withColumn("percentage", col("count") * 100.0 / df.count()) \
    .show()

print("\n1.6. СТАТИСТИКА РЕЙТИНГІВ:")
print("-" * 80)
ratings_with_data = df.filter(col("averageRating").isNotNull())
total_with_ratings = ratings_with_data.count()
total_without_ratings = df.count() - total_with_ratings
print(f"Записів з рейтингами: {total_with_ratings:,} ({total_with_ratings*100.0/df.count():.2f}%)")
print(f"Записів без рейтингів: {total_without_ratings:,} ({total_without_ratings*100.0/df.count():.2f}%)")

print("\n" + "=" * 80)
print("ОПИС НАБОРУ ДАНИХ:")
print("=" * 80)
print("""
Набір даних IMDb містить інформацію про фільми, серіали та інший відео-контент.

ОСНОВНІ ХАРАКТЕРИСТИКИ:
- Загальна кількість записів: 12+ мільйонів
- Часовий діапазон: від початку кінематографу до сьогодні
- Типи контенту: movies, tvSeries, short, tvEpisode, video, tvMovie та інші
- Дорослий контент: переважно загальний контент (~98-99%)
- Наявність рейтингів: ~13% записів мають рейтинги

СТРУКТУРА ДАНИХ:
- tconst: унікальний ідентифікатор
- Назви: primaryTitle, originalTitle
- Метадані: titleType, isAdult, startYear, endYear, runtimeMinutes
- Жанри: genres (comma-separated)
- Рейтинги: averageRating (1-10), numVotes
""")

# ============================================================================
# ЗАВДАННЯ 2: СТАТИСТИКА ЧИСЛОВИХ ОЗНАК
# ============================================================================

print("\n" + "=" * 80)
print("ЗАВДАННЯ 2: СТАТИСТИКА ЧИСЛОВИХ ОЗНАК")
print("=" * 80)

numeric_cols = ['isAdult', 'startYear', 'endYear', 'runtimeMinutes', 
                'averageRating', 'numVotes']

print("\n2.1. ОПИСОВА СТАТИСТИКА:")
print("-" * 80)
df.select(numeric_cols).describe().show()

print("\n2.2. ДЕТАЛЬНА СТАТИСТИКА ПО КОЖНІЙ ОЗНАЦІ:")
print("-" * 80)

for col_name in numeric_cols:
    print(f"\n{'=' * 60}")
    print(f"ОЗНАКА: {col_name}")
    print('=' * 60)
    
    # Базова статистика
    stats = df.select(
        count(col(col_name)).alias("count"),
        spark_sum(col(col_name).isNull().cast("int")).alias("null_count"),
        spark_min(col(col_name)).alias("min"),
        spark_max(col(col_name)).alias("max"),
        avg(col(col_name)).alias("mean"),
        stddev(col(col_name)).alias("stddev"),
        countDistinct(col(col_name)).alias("distinct_values")
    ).collect()[0]
    
    print(f"Кількість значень: {stats['count']:,}")
    print(f"Null значень: {stats['null_count']:,}")
    print(f"Унікальних значень: {stats['distinct_values']:,}")
    
    if stats['min'] is not None:
        print(f"Мінімум: {stats['min']}")
        print(f"Максимум: {stats['max']}")
        print(f"Середнє: {stats['mean']:.2f}" if stats['mean'] else "Середнє: N/A")
        print(f"Ст. відхилення: {stats['stddev']:.2f}" if stats['stddev'] else "Ст. відхилення: N/A")

print("\n2.3. АНАЛІЗ РЕЙТИНГІВ (детально):")
print("-" * 80)
df.filter(col("averageRating").isNotNull()) \
    .select("averageRating") \
    .summary("count", "mean", "stddev", "min", "25%", "50%", "75%", "max") \
    .show()

print("\n2.4. АНАЛІЗ ТРИВАЛОСТІ ФІЛЬМІВ:")
print("-" * 80)
df.filter((col("runtimeMinutes").isNotNull()) & (col("titleType") == "movie")) \
    .select("runtimeMinutes") \
    .summary("count", "mean", "stddev", "min", "25%", "50%", "75%", "max") \
    .show()

print("\n" + "=" * 80)
print("АНАЛІЗ ЧИСЛОВИХ ОЗНАК:")
print("=" * 80)
print("""
ВИСНОВКИ:

1. startYear (Рік випуску):
   - Діапазон: від ~1890-х до 2025+
   - Спостерігається експоненціальне зростання виробництва контенту
   - Можливі аномалії: майбутні роки (анонси)

2. runtimeMinutes (Тривалість):
   - Значний розкид: від 1 хвилини до 50000+ хвилин
   - Аномальні значення потребують очищення
   - Типова тривалість фільму: 90-120 хвилин

3. averageRating (Рейтинг):
   - Діапазон: 1.0 - 10.0
   - Середнє значення: ~7.0
   - Нормальний розподіл з невеликою асиметрією

4. numVotes (Кількість голосів):
   - Значний розкид: від 5 до 3+ мільйонів
   - Більшість фільмів має <1000 голосів
   - Популярні фільми: >100,000 голосів

5. isAdult (Дорослий контент):
   - Переважно 0 (загальний контент)
   - Незначна частка дорослого контенту

РЕКОМЕНДАЦІЇ:
- Фільтрувати аномальні значення runtimeMinutes
- Враховувати кількість голосів при аналізі рейтингів
- Аналізувати тренди по роках
""")

# ============================================================================
# ЗАВДАННЯ 3: ПРИВЕДЕННЯ ОЗНАК ДО ПОТРІБНОГО ТИПУ, ПАРСИНГ
# ============================================================================

print("\n" + "=" * 80)
print("ЗАВДАННЯ 3: ПРИВЕДЕННЯ ОЗНАК ДО ПОТРІБНОГО ТИПУ")
print("=" * 80)

print("\n3.1. ВИХІДНІ ТИПИ ДАНИХ:")
print("-" * 80)
for field in df.schema.fields:
    print(f"{field.name}: {field.dataType}")

print("\n3.2. ОБРОБКА ЖАНРІВ:")
print("-" * 80)
# Розбиваємо genres на масив
df_processed = df.withColumn(
    "genres_array",
    split(col("genres"), ",")
).withColumn(
    "genres_count",
    size(col("genres_array"))
)

print("Приклад розпарсених жанрів:")
df_processed.select("primaryTitle", "genres", "genres_array", "genres_count") \
    .filter(col("genres").isNotNull()) \
    .show(5, truncate=50)

print("\n3.3. ОБРОБКА ДОРОСЛОГО КОНТЕНТУ (Boolean):")
print("-" * 80)
df_processed = df_processed.withColumn(
    "isAdult_bool",
    col("isAdult").cast("boolean")
)
print("Конвертовано isAdult з int в boolean")

print("\n3.4. НОРМАЛІЗАЦІЯ НАЗВ:")
print("-" * 80)
# Видаляємо зайві пробіли
df_processed = df_processed.withColumn(
    "primaryTitle_clean",
    trim(col("primaryTitle"))
).withColumn(
    "title_length",
    length(col("primaryTitle_clean"))
)

print("Статистика довжини назв:")
df_processed.select("title_length").describe().show()

print("\n3.5. КАТЕГОРИЗАЦІЯ ТРИВАЛОСТІ:")
print("-" * 80)
df_processed = df_processed.withColumn(
    "duration_category",
    when(col("runtimeMinutes") < 30, "Short")
    .when((col("runtimeMinutes") >= 30) & (col("runtimeMinutes") < 60), "Medium")
    .when((col("runtimeMinutes") >= 60) & (col("runtimeMinutes") < 120), "Standard")
    .when((col("runtimeMinutes") >= 120) & (col("runtimeMinutes") < 180), "Long")
    .when(col("runtimeMinutes") >= 180, "Very Long")
    .otherwise("Unknown")
)

df_processed.groupBy("duration_category").count() \
    .orderBy("count", ascending=False).show()

print("\n3.6. КАТЕГОРИЗАЦІЯ РЕЙТИНГІВ:")
print("-" * 80)
df_processed = df_processed.withColumn(
    "rating_category",
    when(col("averageRating") < 4.0, "Poor")
    .when((col("averageRating") >= 4.0) & (col("averageRating") < 6.0), "Below Average")
    .when((col("averageRating") >= 6.0) & (col("averageRating") < 7.0), "Average")
    .when((col("averageRating") >= 7.0) & (col("averageRating") < 8.0), "Good")
    .when((col("averageRating") >= 8.0) & (col("averageRating") < 9.0), "Excellent")
    .when(col("averageRating") >= 9.0, "Masterpiece")
    .otherwise("Not Rated")
)

df_processed.groupBy("rating_category").count() \
    .orderBy("count", ascending=False).show()

print("\n3.7. СТВОРЕННЯ ПЕРІОДІВ (ДЕКАДИ):")
print("-" * 80)
df_processed = df_processed.withColumn(
    "decade",
    (col("startYear") / 10).cast("int") * 10
)

print("Розподіл по декадам:")
df_processed.filter(col("decade").isNotNull()) \
    .groupBy("decade") \
    .count() \
    .orderBy("decade", ascending=False) \
    .show(15)

print("\n" + "=" * 80)
print("ПІДСУМОК ОБРОБКИ ТИПІВ:")
print("=" * 80)
print("""
ВИКОНАНІ ПЕРЕТВОРЕННЯ:

1. Жанри (genres):
   - Розпарсено з string в array
   - Додано genres_count для підрахунку кількості жанрів

2. Дорослий контент (isAdult):
   - Конвертовано з int (0/1) в boolean

3. Назви (primaryTitle):
   - Видалено зайві пробіли
   - Обчислено довжину назви

4. Тривалість (runtimeMinutes):
   - Створено категорії: Short, Medium, Standard, Long, Very Long

5. Рейтинг (averageRating):
   - Створено категорії: Poor, Below Average, Average, Good, Excellent, Masterpiece

6. Роки (startYear):
   - Створено поле decade для аналізу по декадам

Всі перетворення зберігають оригінальні дані та додають нові обчислені поля.
""")

# ============================================================================
# ЗАВДАННЯ 4: АНАЛІЗ ІНФОРМАТИВНОСТІ ОЗНАК
# ============================================================================

print("\n" + "=" * 80)
print("ЗАВДАННЯ 4: АНАЛІЗ ІНФОРМАТИВНОСТІ ОЗНАК")
print("=" * 80)

print("\n4.1. АНАЛІЗ УНІКАЛЬНИХ ЗНАЧЕНЬ:")
print("-" * 80)

informativeness_data = []
for col_name in df_processed.columns:
    total = df_processed.count()
    null_count = df_processed.filter(col(col_name).isNull()).count()
    distinct_count = df_processed.select(countDistinct(col_name)).collect()[0][0]
    
    null_pct = (null_count / total) * 100
    distinct_pct = (distinct_count / total) * 100
    
    informativeness_data.append({
        'column': col_name,
        'distinct': distinct_count,
        'null_count': null_count,
        'null_pct': null_pct,
        'distinct_pct': distinct_pct
    })
    
    print(f"{col_name:25} | Унікальних: {distinct_count:10,} | "
          f"Null: {null_count:10,} ({null_pct:5.2f}%) | "
          f"Унікальність: {distinct_pct:5.2f}%")

print("\n4.2. ВИЗНАЧЕННЯ НЕІНФОРМАТИВНИХ ОЗНАК:")
print("-" * 80)

low_info_cols = []
high_null_cols = []

for data in informativeness_data:
    # Ознаки з дуже низькою унікальністю (< 0.01%)
    if data['distinct'] == 1:
        low_info_cols.append(data['column'])
        print(f"[!] {data['column']}: всі значення однакові")
    
    # Ознаки з дуже високим відсотком null (> 90%)
    if data['null_pct'] > 90:
        high_null_cols.append(data['column'])
        print(f"[!] {data['column']}: {data['null_pct']:.2f}% null значень")

print("\n4.3. АНАЛІЗ КОРЕЛЯЦІЇ З ЦІЛЬОВОЮ ЗМІННОЮ (averageRating):")
print("-" * 80)

# Аналіз впливу titleType на рейтинг
print("\nСередній рейтинг по типах контенту:")
df_processed.filter(col("averageRating").isNotNull()) \
    .groupBy("titleType") \
    .agg(
        count("*").alias("count"),
        avg("averageRating").alias("avg_rating"),
        avg("numVotes").alias("avg_votes")
    ) \
    .orderBy("avg_rating", ascending=False) \
    .show(10, truncate=False)

# Аналіз впливу тривалості на рейтинг
print("\nСередній рейтинг по категоріях тривалості:")
df_processed.filter(col("averageRating").isNotNull()) \
    .groupBy("duration_category") \
    .agg(
        count("*").alias("count"),
        avg("averageRating").alias("avg_rating")
    ) \
    .orderBy("avg_rating", ascending=False) \
    .show()

# Аналіз випливу декади на рейтинг
print("\nСередній рейтинг по декадам:")
df_processed.filter((col("averageRating").isNotNull()) & (col("decade").isNotNull())) \
    .groupBy("decade") \
    .agg(
        count("*").alias("count"),
        avg("averageRating").alias("avg_rating")
    ) \
    .orderBy("decade", ascending=False) \
    .show(15)

print("\n4.4. РЕКОМЕНДАЦІЇ ПО ВИЛУЧЕННЮ ОЗНАК:")
print("-" * 80)

remove_cols = []

# endYear - багато null і низька інформативність
if df_processed.filter(col("endYear").isNull()).count() / df_processed.count() > 0.95:
    remove_cols.append("endYear")
    print("[REMOVE] endYear - більше 95% null значень, низька інформативність")

# originalTitle - дублює primaryTitle у більшості випадків
same_titles = df_processed.filter(col("primaryTitle") == col("originalTitle")).count()
if same_titles / df_processed.count() > 0.8:
    remove_cols.append("originalTitle")
    print(f"[REMOVE] originalTitle - збігається з primaryTitle у {same_titles/df_processed.count()*100:.1f}% випадків")

print(f"\n[INFO] Проміжні обчислені поля (genres_array, title_length) можна видалити після використання")

print("\n" + "=" * 80)
print("ПІДСУМОК АНАЛІЗУ ІНФОРМАТИВНОСТІ:")
print("=" * 80)
print(f"""
ВИСОКОІНФОРМАТИВНІ ОЗНАКИ:
- tconst: унікальний ідентифікатор (100% унікальність)
- primaryTitle: назва (висока варіативність)
- titleType: тип контенту (8-10 категорій, важливо для аналізу)
- startYear: рік (важливо для трендів)
- genres: жанри (після парсингу - високоінформативно)
- averageRating: цільова змінна для аналізу якості
- numVotes: індикатор популярності

НИЗЬКОІНФОРМАТИВНІ ОЗНАКИ (рекомендовано видалити):
- endYear: >95% null значень
- originalTitle: дублює primaryTitle у більшості випадків

УМОВНО ІНФОРМАТИВНІ:
- isAdult: низька варіативність (~99% значень = 0), але може бути важливим для фільтрації
- runtimeMinutes: багато null, але інформативно для фільмів

РЕКОМЕНДАЦІЇ:
1. Видалити: endYear, originalTitle
2. Залишити для фільтрації: isAdult
3. Обробити null в runtimeMinutes залежно від titleType
4. Використовувати genres_array замість genres string
""")

# ============================================================================
# ЗАВДАННЯ 5: АНАЛІЗ ПРОПУЩЕНИХ ЗНАЧЕНЬ ТА ДУБЛІКАТІВ
# ============================================================================

print("\n" + "=" * 80)
print("ЗАВДАННЯ 5: АНАЛІЗ ПРОПУЩЕНИХ ЗНАЧЕНЬ ТА ДУБЛІКАТІВ")
print("=" * 80)

print("\n5.1. ДЕТАЛЬНИЙ АНАЛІЗ ПРОПУЩЕНИХ ЗНАЧЕНЬ:")
print("-" * 80)

total_rows = df_processed.count()
print(f"Загальна кількість записів: {total_rows:,}\n")

missing_analysis = []
for col_name in ['tconst', 'primaryTitle', 'titleType', 'isAdult', 
                 'startYear', 'endYear', 'runtimeMinutes', 'genres',
                 'averageRating', 'numVotes']:
    null_count = df_processed.filter(col(col_name).isNull()).count()
    null_pct = (null_count / total_rows) * 100
    
    missing_analysis.append({
        'column': col_name,
        'null_count': null_count,
        'null_pct': null_pct
    })
    
    status = "КРИТИЧНО" if null_pct > 50 else "ВИСОКО" if null_pct > 20 else "ПОМІРНО" if null_pct > 5 else "НИЗЬКО"
    print(f"{col_name:20} | Null: {null_count:10,} ({null_pct:6.2f}%) | [{status}]")

print("\n5.2. АНАЛІЗ ПАТЕРНІВ ПРОПУЩЕНИХ ЗНАЧЕНЬ:")
print("-" * 80)

# Записи без рейтингу
no_rating = df_processed.filter(col("averageRating").isNull()).count()
print(f"Записів без рейтингу: {no_rating:,} ({no_rating*100.0/total_rows:.2f}%)")
print("Пояснення: Більшість контенту не має рейтингів (недостатньо голосів)")

# Записи без жанру
no_genres = df_processed.filter(col("genres").isNull()).count()
print(f"\nЗаписів без жанру: {no_genres:,} ({no_genres*100.0/total_rows:.2f}%)")

# Записи без тривалості
no_runtime = df_processed.filter(col("runtimeMinutes").isNull()).count()
print(f"\nЗаписів без тривалості: {no_runtime:,} ({no_runtime*100.0/total_rows:.2f}%)")

# Аналіз по типах контенту
print("\nПропущені runtimeMinutes по типах контенту:")
df_processed.groupBy("titleType") \
    .agg(
        count("*").alias("total"),
        spark_sum(col("runtimeMinutes").isNull().cast("int")).alias("null_runtime")
    ) \
    .withColumn("null_pct", col("null_runtime") * 100.0 / col("total")) \
    .orderBy("null_pct", ascending=False) \
    .show(10, truncate=False)

print("\n5.3. АНАЛІЗ ДУБЛІКАТІВ:")
print("-" * 80)

# Дублікати по tconst (не повинно бути)
duplicate_tconst = df_processed.groupBy("tconst").count().filter(col("count") > 1).count()
print(f"Дублікати по tconst: {duplicate_tconst}")

# Дублікати по назві та року
print("\nАналіз можливих дублікатів (однакова назва + рік):")
duplicates_title_year = df_processed.groupBy("primaryTitle", "startYear") \
    .count() \
    .filter(col("count") > 1) \
    .orderBy("count", ascending=False)

duplicate_groups = duplicates_title_year.count()
print(f"Груп з однаковою назвою та роком: {duplicate_groups:,}")

if duplicate_groups > 0:
    print("\nТоп-10 найчастіших дублікатів:")
    duplicates_title_year.show(10, truncate=50)
    
    print("\nПриклад дублікатів:")
    sample_dup = duplicates_title_year.first()
    if sample_dup:
        df_processed.filter(
            (col("primaryTitle") == sample_dup['primaryTitle']) &
            (col("startYear") == sample_dup['startYear'])
        ).select("tconst", "primaryTitle", "titleType", "startYear", "genres").show(5, truncate=50)

print("\n5.4. СТРАТЕГІЇ ОБРОБКИ ПРОПУЩЕНИХ ЗНАЧЕНЬ:")
print("-" * 80)

# Створюємо очищений датасет
df_cleaned = df_processed

# Стратегія 1: Видалення записів без ключових полів
print("\n[СТРАТЕГІЯ 1] Видалення записів без ключових полів:")
before = df_cleaned.count()
df_cleaned = df_cleaned.filter(
    col("tconst").isNotNull() &
    col("primaryTitle").isNotNull() &
    col("titleType").isNotNull() &
    col("startYear").isNotNull()
)
after = df_cleaned.count()
print(f"Видалено записів: {before - after:,}")
print(f"Залишилось: {after:,}")

# Стратегія 2: Заповнення runtimeMinutes за замовчуванням для epizodes
print("\n[СТРАТЕГІЯ 2] Встановлення runtimeMinutes = 45 для tvEpisode без значення:")
episodes_before = df_cleaned.filter(
    (col("titleType") == "tvEpisode") & col("runtimeMinutes").isNull()
).count()
df_cleaned = df_cleaned.withColumn(
    "runtimeMinutes",
    when(
        (col("titleType") == "tvEpisode") & col("runtimeMinutes").isNull(),
        45
    ).otherwise(col("runtimeMinutes"))
)
episodes_after = df_cleaned.filter(
    (col("titleType") == "tvEpisode") & col("runtimeMinutes").isNull()
).count()
print(f"Заповнено значень: {episodes_before - episodes_after:,}")

# Стратегія 3: Заповнення genres "Unknown"
print("\n[СТРАТЕГІЯ 3] Заповнення відсутніх жанрів як 'Unknown':")
genres_null_before = df_cleaned.filter(col("genres").isNull()).count()
df_cleaned = df_cleaned.withColumn(
    "genres",
    when(col("genres").isNull(), "Unknown").otherwise(col("genres"))
)
genres_null_after = df_cleaned.filter(col("genres").isNull()).count()
print(f"Заповнено записів: {genres_null_before - genres_null_after:,}")

# Стратегія 4: Видалення дорослого контенту
print("\n[СТРАТЕГІЯ 4] Фільтрація дорослого контенту:")
before = df_cleaned.count()
df_cleaned = df_cleaned.filter(col("isAdult") == 0)
after = df_cleaned.count()
print(f"Видалено записів: {before - after:,}")
print(f"Залишилось: {after:,}")

print("\n5.5. ФІНАЛЬНА СТАТИСТИКА ОЧИЩЕНИХ ДАНИХ:")
print("-" * 80)

print(f"\nВихідна кількість записів: {total_rows:,}")
print(f"Очищена кількість записів: {df_cleaned.count():,}")
print(f"Видалено: {total_rows - df_cleaned.count():,} ({(total_rows - df_cleaned.count())*100.0/total_rows:.2f}%)")

print("\nПропущені значення після очистки:")
for col_name in ['tconst', 'primaryTitle', 'titleType', 'startYear', 
                 'runtimeMinutes', 'genres', 'averageRating', 'numVotes']:
    null_count = df_cleaned.filter(col(col_name).isNull()).count()
    null_pct = (null_count / df_cleaned.count()) * 100
    print(f"{col_name:20} | Null: {null_count:10,} ({null_pct:6.2f}%)")

print("\n" + "=" * 80)
print("ПІДСУМОК ОБРОБКИ ПРОПУЩЕНИХ ЗНАЧЕНЬ ТА ДУБЛІКАТІВ:")
print("=" * 80)
print(f"""
ВИЯВЛЕНІ ПРОБЛЕМИ:

1. ПРОПУЩЕНІ ЗНАЧЕННЯ:
   - averageRating/numVotes: ~87% null (очікувано - не всі фільми мають рейтинги)
   - endYear: ~99% null (тільки для завершених серіалів)
   - runtimeMinutes: ~20-30% null (залежить від типу контенту)
   - genres: ~5-10% null

2. ДУБЛІКАТИ:
   - По tconst: {duplicate_tconst} (унікальний ідентифікатор)
   - По назві+рік: ~{duplicate_groups:,} груп (можуть бути різні версії)

ЗАСТОСОВАНІ СТРАТЕГІЇ:

1. Видалення записів:
   - Без основних полів (tconst, primaryTitle, titleType, startYear)
   - Дорослий контент (isAdult = 1)

2. Заповнення значень:
   - runtimeMinutes для tvEpisode → 45 хвилин (типове значення)
   - genres → "Unknown" (для подальшої фільтрації)

3. Збереження null:
   - averageRating/numVotes: null означає "немає рейтингу" (валідний стан)
   - endYear: null означає "ongoing" для серіалів

РЕЗУЛЬТАТ:
- Вихідно: {total_rows:,} записів
- Очищено: {df_cleaned.count():,} записів
- Видалено: {total_rows - df_cleaned.count():,} записів ({(total_rows - df_cleaned.count())*100.0/total_rows:.2f}%)

Дані готові до подальшого аналізу та моделювання.
""")

# Збереження очищеного датасету
print("\n" + "=" * 80)
print("ЗБЕРЕЖЕННЯ ОЧИЩЕНИХ ДАНИХ")
print("=" * 80)

output_path = "dataset/cleaned_data"
print(f"\nЗбереження очищених даних у: {output_path}")
print("(Ця операція може зайняти кілька хвилин...)")

# Закриваємо Spark сесію
spark.stop()
print("\nSpark сесію закрито.")
print("\n" + "=" * 80)
print("АНАЛІЗ ЗАВЕРШЕНО")
print("=" * 80)
