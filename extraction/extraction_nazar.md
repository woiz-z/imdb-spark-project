# Схеми та функції завантаження (name.basics, title.crew)

**Автор:** Яйко Назар  
**Етап:** Видобування даних  

## Створені схеми

### name.basics.tsv
```python
schema_name_basics = StructType([
    StructField("nconst", StringType(), True),
    StructField("primaryName", StringType(), True),
    StructField("birthYear", IntegerType(), True),
    StructField("deathYear", IntegerType(), True),
    StructField("primaryProfession", StringType(), True),
    StructField("knownForTitles", StringType(), True)
])
```

### title.crew.tsv
```python
schema_title_crew = StructType([
    StructField("tconst", StringType(), True),
    StructField("directors", StringType(), True),
    StructField("writers", StringType(), True)
])
```

## Функції завантаження

- `load_name_basics()` - 15,085,440 записів ✓
- `load_title_crew()` - 12,287,568 записів ✓

**Статус:** Завершено
