# Аналіз числових ознак та викидів

**Автор:** Чігур Володимир | **Етап:** Preprocessing

```python
# Статистика числових полів
df.select("startYear", "runtimeMinutes", "averageRating") \
  .summary("min", "max", "mean", "stddev").show()

# Виявлення викидів (IQR method)
# Виявлено: runtimeMinutes > 300 хв, startYear < 1900
```

**Статус:** Викиди виявлені та задокументовані ✓
