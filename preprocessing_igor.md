# Приведення типів та парсинг полів

**Автор:** Качмар Ігор | **Етап:** Preprocessing

```python
# Приведення типів
df = df.withColumn("startYear", col("startYear").cast(IntegerType()))
df = df.withColumn("averageRating", col("averageRating").cast(DoubleType()))

# Парсинг genres
df = df.withColumn("genres_array", split(col("genres"), ","))
```

**Статус:** Типи приведені, складні поля розпарсені ✓
