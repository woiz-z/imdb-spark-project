# Бізнес-питання з JOIN операціями

**Автор:** Іванчук Орест | **Етап:** Трансформація

## 6 бізнес-питань з використанням JOIN

### 1. Топ режисери за кількістю високорейтингових фільмів
```python
df_top_directors = df_titles \
    .join(df_ratings, "tconst") \
    .join(df_crew, "tconst") \
    .join(df_names, df_crew.directors == df_names.nconst) \
    .where(col("averageRating") >= 8.0) \
    .groupBy("primaryName") \
    .count() \
    .orderBy(col("count").desc())
```

### 2. Фільми з акторами та їх рейтинг
```python
df_actor_movies = df_titles \
    .join(df_ratings, "tconst") \
    .join(df_principals, "tconst") \
    .join(df_names, df_principals.nconst == df_names.nconst) \
    .select("primaryTitle", "primaryName", "averageRating")
```

### 3. Broadcast join для малих таблиць (ratings)
```python
from pyspark.sql.functions import broadcast

df_optimized = df_titles \
    .join(broadcast(df_ratings), "tconst")
```

### 4. JOIN всіх таблиць
```python
df_full = df_titles \
    .join(df_ratings, "tconst", "left") \
    .join(df_crew, "tconst", "left")
```

### 5. Self-join для порівняння
```python
df_compare = df_ratings.alias("a") \
    .join(df_ratings.alias("b"), col("a.numVotes") == col("b.numVotes")) \
    .where(col("a.tconst") != col("b.tconst"))
```

### 6. Multiple joins оптимізація
```python
df_complex = df_titles \
    .join(df_ratings, "tconst") \
    .join(df_crew, "tconst") \
    .where(col("averageRating") > 8.0)
```

## Аналіз Physical Plan

```python
df_optimized.explain()
```

**Виявлено:** BroadcastHashJoin для малих таблиць, SortMergeJoin для великих ✓

**Статус:** 6 бізнес-питань з JOIN створено ✓
