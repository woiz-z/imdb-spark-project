# Аналіз інформативності ознак

**Автор:** Іванчук Орест | **Етап:** Preprocessing

```python
# Перевірка інформативності
df.select("endYear").distinct().count()  # 95% NULL - неінформативне

# Видалення
df = df.drop("endYear", "originalTitle")
```

**Видалено:** endYear (95% NULL), originalTitle (дублікат) ✓
