# Бізнес-питання з WHERE умовами

**Автор:** Качмар Ігор | **Етап:** Трансформація

## 6 бізнес-питань з використанням WHERE

### 1. Топ-10 фільмів після 2010 року
```python
df_top_2010 = df_joined \
    .where(col("startYear") >= 2010) \
    .where(col("numVotes") > 10000) \
    .orderBy(col("averageRating").desc()) \
    .limit(10)

df_top_2010.select("primaryTitle", "startYear", "averageRating").show()
```

### 2. Фільми жанру Drama з рейтингом > 8.0
```python
df_drama = df \
    .where(col("genres").contains("Drama")) \
    .where(col("averageRating") > 8.0) \
    .select("primaryTitle", "averageRating", "genres")
```

### 3. Короткі фільми (<90 хв) з високим рейтингом
```python
df_short_high = df \
    .where(col("runtimeMinutes") < 90) \
    .where(col("averageRating") >= 7.5) \
    .where(col("numVotes") > 5000)
```

### 4. Фільми 1990-х років
```python
df_90s = df \
    .where((col("startYear") >= 1990) & (col("startYear") < 2000)) \
    .where(col("titleType") == "movie")
```

### 5. Непопулярні фільми (мало голосів)
```python
df_unpopular = df \
    .where(col("numVotes") < 100) \
    .where(col("startYear") >= 2000)
```

### 6. Фільми з екстремальними рейтингами
```python
df_extreme = df \
    .where((col("averageRating") >= 9.0) | (col("averageRating") <= 3.0)) \
    .where(col("numVotes") > 1000)
```

## Аналіз Execution Plan

```python
df_top_2010.explain()
```

**Виявлено:** Filter pushdown оптимізація, 2 етапи (Filter → Sort → Limit) ✓

**Статус:** 6 бізнес-питань створено та проаналізовано ✓
