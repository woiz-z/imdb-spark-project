# Бізнес-питання з Window функціями

**Автор:** Яйко Назар | **Етап:** Трансформація

## 6 бізнес-питань з використанням Window функцій

### 1. Топ-3 фільми по кожному жанру
```python
from pyspark.sql.window import Window

window_spec = Window.partitionBy("genres").orderBy(col("averageRating").desc())

df_top3_genre = df \
    .withColumn("rank", row_number().over(window_spec)) \
    .where(col("rank") <= 3)
```

### 2. Ранжування фільмів по рейтингу (rank vs dense_rank)
```python
df_ranked = df \
    .withColumn("rank", rank().over(Window.orderBy(col("averageRating").desc()))) \
    .withColumn("dense_rank", dense_rank().over(Window.orderBy(col("averageRating").desc())))
```

### 3. Накопичувальна сума голосів по роках
```python
window_cumsum = Window.orderBy("startYear").rowsBetween(Window.unboundedPreceding, 0)

df_cumulative = df \
    .withColumn("cumulative_votes", sum("numVotes").over(window_cumsum))
```

### 4. Середнє по вікну (moving average)
```python
window_ma = Window.orderBy("startYear").rows Between(-2, 2)

df_moving_avg = df \
    .withColumn("ma_rating", avg("averageRating").over(window_ma))
```

### 5. Percentile по кожному року
```python
window_year = Window.partitionBy("startYear")

df_percentile = df \
    .withColumn("percentile", percent_rank().over(window_year.orderBy("averageRating")))
```

### 6. Lag/Lead для порівняння з попереднім
```python
window_lag = Window.partitionBy("genres").orderBy("startYear")

df_compare = df \
    .withColumn("prev_rating", lag("averageRating", 1).over(window_lag)) \
    .withColumn("next_rating", lead("averageRating", 1).over(window_lag))
```

## Аналіз партиціонування та сортування

```python
df_top3_genre.explain()
```

**Виявлено:** Window оператор з партиціонуванням, сортування всередині партіцій ✓

**Статус:** 6 бізнес-питань з Window створено ✓
