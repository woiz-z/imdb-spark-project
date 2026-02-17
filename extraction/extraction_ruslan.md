# Створення схем та функцій завантаження (title.basics, title.ratings)

**Автор:** Манюк Руслан  
**Етап:** Видобування даних (Extraction)  
**Дата:** Лютий 2026

## Створені схеми PySpark

### 1. Схема для title.basics.tsv

```python
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType

schema_title_basics = StructType([
    StructField("tconst", StringType(), True),
    StructField("titleType", StringType(), True),
    StructField("primaryTitle", StringType(), True),
    StructField("originalTitle", StringType(), True),
    StructField("isAdult", BooleanType(), True),
    StructField("startYear", IntegerType(), True),
    StructField("endYear", IntegerType(), True),
    StructField("runtimeMinutes", IntegerType(), True),
    StructField("genres", StringType(), True)
])
```

### 2. Схема для title.ratings.tsv

```python
schema_title_ratings = StructType([
    StructField("tconst", StringType(), True),
    StructField("averageRating", DoubleType(), True),
    StructField("numVotes", IntegerType(), True)
])
```

## Імплементовані функції

### load_title_basics()

```python
def load_title_basics(spark, file_path="dataset/title.basics.tsv"):
    """Завантаження основної інформації про фільми"""
    df = spark.read \
        .option("header", "true") \
        .option("delimiter", "\t") \
        .option("nullValue", "\\N") \
        .schema(schema_title_basics) \
        .csv(file_path)
    
    return df
```

### load_title_ratings()

```python
def load_title_ratings(spark, file_path="dataset/title.ratings.tsv"):
    """Завантаження рейтингів фільмів"""
    df = spark.read \
        .option("header", "true") \
        .option("delimiter", "\t") \
        .option("nullValue", "\\N") \
        .schema(schema_title_ratings) \
        .csv(file_path)
    
    return df
```

## Тестування

```python
# Завантаження даних
df_basics = load_title_basics(spark)
df_ratings = load_title_ratings(spark)

# Перевірка
print(f"title.basics: {df_basics.count():,} records")
print(f"title.ratings: {df_ratings.count():,} records")

df_basics.show(5)
df_ratings.show(5)
```

**Результат:**
- title.basics: 12,287,568 записів ✓
- title.ratings: 1,634,950 записів ✓

**Статус:** Завершено
