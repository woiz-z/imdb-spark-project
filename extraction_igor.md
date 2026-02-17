# Валідація DataFrame та перевірка коректності

**Автор:** Качмар Ігор  
**Етап:** Видобування даних

## Функція validate_dataframe()

```python
def validate_dataframe(df, name):
    """Перевірка коректності завантаження DataFrame"""
    print(f"\n=== Валідація {name} ===")
    print(f"Записів: {df.count():,}")
    print(f"Колонки: {df.columns}")
    df.printSchema()
    df.show(3)
    return True
```

## Результати валідації

| Датасет | Записів | Статус |
|---------|---------|--------|
| title.basics | 12,287,568 | ✓ OK |
| title.ratings | 1,634,950 | ✓ OK |
| name.basics | 15,085,440 | ✓ OK |
| title.crew | 12,287,568 | ✓ OK |

**Статус:** Всі DataFrame валідовані ✓
