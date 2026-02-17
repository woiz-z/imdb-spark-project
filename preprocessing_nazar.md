# Статистичний аналіз та dataset_analysis.py

**Автор:** Яйко Назар | **Етап:** Preprocessing

## Створено dataset_analysis.py

```python
# Статистика методом .describe()
df_ratings.describe().show()

# Аналіз розподілу
df_ratings.groupBy("averageRating").count().orderBy("averageRating").show()
df_ratings.select(avg("numVotes"), stddev("numVotes")).show()
```

**Результати:** Виявлено 1.6M рейтингів, середній рейтинг 7.1, медіана голосів ~150 ✓
