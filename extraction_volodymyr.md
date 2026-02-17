# Модуль data_loader.py та обробка NULL значень

**Автор:** Чігур Володимир  
**Етап:** Видобування даних

## Розроблений модуль data_loader.py

Включає всі функції завантаження та схеми:
- schema_title_basics, load_title_basics()
- schema_title_ratings, load_title_ratings()
- schema_name_basics, load_name_basics()
- schema_title_crew, load_title_crew()
- schema_title_principals, load_title_principals()
- schema_title_akas, load_title_akas()

## Обробка NULL (`\N`)

```python
df = spark.read \
    .option("nullValue", "\\N") \  # IMDb використовує \N для NULL
    .schema(schema) \
    .csv(file_path)
```

**Статус:** Завершено. Модуль готовий.
