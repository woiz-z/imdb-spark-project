# Фінальна документація BUSINESS_QUESTIONS.md

**Автор:** Чігур Володимир | **Етап:** Трансформація

## Створено BUSINESS_QUESTIONS.md

Документація всіх 30 бізнес-питань (по 6 від кожного учасника):

### Розподіл по категоріях:

1. **Качмар Ігор** - 6 питань 
2. **Іванчук Орест** - 6 питань 
3. **Манюк Руслан** - 6 питань 
4. **Яйко Назар** - 6 питань 
5. **Чігур Володимир** - 6 питань

## Аналіз та оптимізація

Для кожного питання виконано:
- `.explain()` - аналіз execution plan
- Виявлення shuffle операцій
- Оптимізація запитів
- Документування physical plan

### Приклад комбінованого питання:

```python
# Топ-10 режисерів з найбільшою кількістю високорейтингових фільмів
df_result = df_titles \
    .join(broadcast(df_ratings), "tconst") \
    .join(df_crew, "tconst") \
    .where(col("averageRating") >= 8.0) \
    .where(col("numVotes") > 10000) \
    .groupBy(df_crew.directors) \
    .agg(
        count("*").alias("high_rated_count"),
        avg("averageRating").alias("avg_rating")
    ) \
    .join(df_names, df_crew.directors == df_names.nconst) \
    .select(
        col("primaryName").alias("director"),
        col("high_rated_count"),
        col("avg_rating")
    ) \
    .orderBy(col("high_rated_count").desc()) \
    .limit(10)

df_result.explain(mode="extended")
```

## Фінальне тестування

Всі 30 бізнес-питань протестовано:
- ✓ Синтаксис коректний
- ✓ Execution plan проаналізовано
- ✓ Результати валідні
- ✓ Документація створена

**Статус:** Проєкт завершено. Всі етапи виконано ✓
