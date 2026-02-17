# Бізнес-питання з агрегацією

**Автор:** Манюк Руслан | **Етап:** Трансформація

## 6 бізнес-питань з GROUP BY та агрегацією

### 1. Середній рейтинг по роках
```python
df_avg_by_year = df \
    .groupBy("startYear") \
    .agg(
        avg("averageRating").alias("avg_rating"),
        count("*").alias("num_movies")
    ) \
    .orderBy("startYear")
```

### 2. Кількість фільмів по жанрах
```python
df_by_genre = df \
    .groupBy("genres") \
    .count() \
    .orderBy(col("count").desc())
```

### 3. Сума голосів по роках
```python
df_votes_by_year = df \
    .groupBy("startYear") \
    .agg(sum("numVotes").alias("total_votes"))
```

### 4. Максимальний рейтинг по типу фільму
```python
df_max_rating = df \
    .groupBy("titleType") \
    .agg(max("averageRating").alias("max_rating"))
```

### 5. Медіана тривалості по жанрах
```python
from pyspark.sql.functions import expr

df_median = df \
    .groupBy("genres") \
    .agg(expr("percentile_approx(runtimeMinutes, 0.5)").alias("median_runtime"))
```

### 6. Комплексна агрегація
```python
df_complex_agg = df \
    .groupBy("startYear", "titleType") \
    .agg(
        count("*").alias("count"),
        avg("averageRating").alias("avg_rating"),
        sum("numVotes").alias("total_votes"),
        max("runtimeMinutes").alias("max_runtime")
    )
```

## Аналіз shuffle операцій

```python
df_avg_by_year.explain()
```

**Виявлено:** Exchange (shuffle) операція для GROUP BY, HashAggregate для агрегації ✓

**Статус:** 6 бізнес-питань з агрегацією створено ✓
