# Виявлення NULL та дублікатів

**Автор:** Манюк Руслан | **Етап:** Preprocessing

```python
# Аналіз NULL
df.select([count(when(col(c).isNull(), c)).alias(c) for c in df.columns]).show()

# Дублікати
df.groupBy("tconst").count().filter("count > 1").show()  # 0 дублікатів

# Очищення
df_clean = df.na.drop(subset=["tconst", "primaryTitle"])
```

**Статус:** Дані очищені, дублікатів не виявлено ✓
