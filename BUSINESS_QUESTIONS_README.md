# Бізнес-питання IMDb: Інструкція по запуску

## Огляд

30 бізнес-питань для аналізу даних IMDb, розподілених між 5 членами команди (по 6 питань на особу).

## Структура файлів

```
BBD/
├── business_questions.py      # Виконуваний скрипт зі всіма 30 питаннями
├── BUSINESS_QUESTIONS.md      # Детальна документація з explain() аналізом
├── data_loader.py             # Модуль для завантаження даних
└── dataset/                   # IMDb дані (TSV файли)
    ├── title.basics.tsv
    ├── title.ratings.tsv
    ├── title.akas.tsv
    ├── title.crew.tsv
    ├── title.principals.tsv
    ├── name.basics.tsv
    └── title.episode.tsv
```

## Швидкий старт

### 1. Запуск всіх 30 питань

```powershell
# З віртуального середовища
E:/BBD/.venv/Scripts/python.exe business_questions.py
```

⚠️ **Увага:** Повне виконання може зайняти 15-30 хвилин через складність деяких запитів.

### 2. Запуск окремих питань

Відредагуйте `business_questions.py` та закоментуйте непотрібні секції:

```python
# ============================================================================
# ОСОБА 1: Аналіз найкращих фільмів за різними критеріями
# ============================================================================
# Залишити тільки цю секцію, закоментувати інші (ОСОБА 2-5)
```

### 3. Режими explain()

У функції `execute_query()` можна змінити режим аналізу:

```python
# У business_questions.py, в кінці кожного виклику execute_query:

execute_query(..., explain_mode="simple")     # Базовий (за замовчуванням)
execute_query(..., explain_mode="extended")   # Детальний
execute_query(..., explain_mode="formatted")  # Форматований
execute_query(..., explain_mode="cost")       # З оцінкою вартості
```

## Розподіл питань по командах

### ОСОБА 1: Аналіз найкращих фільмів
1. ТОП-10 найрейтинговіших фільмів (Filter + Join)
2. Статистика по жанрах (Group By + Explode)
3. Фільми українською мовою (Filter + 3 Join)
4. Ранжування по декадах (Window Function: row_number)
5. Фільми вище середнього рейтингу жанру (Window: avg)
6. Динаміка 2020-х років (Group By + Custom metric)

### ОСОБА 2: Аналіз акторів та режисерів
1. ТОП-10 режисерів (4 Join + Group By)
2. Актори з різноманітністю жанрів (countDistinct)
3. Активні актори 70+ років (Filter + 3 Join)
4. Ранжування акторів по декадах (Window + 4 Join)
5. **Співпраці режисер-актор (6 Join + Window)** ⚠️ Найскладніше!
6. Продуктивність акторів у жанрах (4 Join + Explode)

### ОСОБА 3: Аналіз серіалів та епізодів
1. ТОП-10 серіалів за рейтингом епізодів (4 Join)
2. Серіали з найбільшою різницею рейтингів (Window: min/max)
3. Епізоди українською (Filter + 4 Join)
4. Динаміка рейтингів по сезонах (Window: lag)
5. Найкращі фінальні сезони (Window: max + Filter)
6. Типи серіалів по декадах (Group By + Filter)

### ОСОБА 4: Географічний та мовний аналіз
1. Найлокалізованіші фільми (countDistinct регіонів)
2. Популярні жанри по країнах (Window + Explode)
3. Фільми з багатьма назвами (collect_set)
4. Рейтинги по мовах (Window: row_number)
5. Еволюція локалізації (Nested Group By)
6. Регіони з унікальним контентом (countDistinct)

### ОСОБА 5: Часовий аналіз та тренди
1. Еволюція тривалості фільмів (Statistical aggregates)
2. Роки з високоякісними фільмами (Filter + Group By)
3. Зростання жанрів (Window: lag + growth rate)
4. Розподіл по категоріям рейтингу (Window: sum for %)
5. Аналіз віку популярних фільмів (Age groups)
6. **Кумулятивне зростання (Window: cumulative sum)** 📊 Цікавий тренд!

## Вимоги виконані ✅

Для кожної особи:
- ✅ Мінімум 3 питання з `filter` (у всіх 6)
- ✅ Мінімум 2 питання з `join` (4-6 у кожного)
- ✅ Мінімум 2 питання з `group by` (4-6 у кожного)
- ✅ Мінімум 2 питання з `window functions` (2-3 у кожного)

**Всього:** 30 питань з детальним explain() аналізом

## Очікувана продуктивність

### Швидкі (<10 сек): 8 питань
- 1.1, 1.3, 1.6
- 3.3, 3.6
- 5.1, 5.2, 5.5

### Середні (10-30 сек): 14 питань
- 1.2, 1.4, 2.3
- 3.1, 3.4, 3.5
- 4.1, 4.2, 4.4, 4.5, 4.6
- 5.4, 5.6

### Повільні (30-60 сек): 5 питань
- 1.5, 2.1, 2.6
- 3.2, 4.3, 5.3

### Дуже повільні (>60 сек): 3 питання
- 2.2, 2.4, **2.5** ⚠️

## Оптимізація виконання

### Збільшити memory для важких запитів:

```python
spark = SparkSession.builder \
    .config("spark.driver.memory", "8g") \
    .config("spark.executor.memory", "8g") \
    .getOrCreate()
```

### Включити адаптивну оптимізацію:

```python
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
```

### Кешування для повторних запитів:

```python
# Після JOIN, перед використанням у декількох запитах
df_movies_ratings = df_basics.join(df_ratings, "tconst").cache()
```

## Аналіз explain()

Кожне питання включає виклик `.explain(mode)`, що показує:

1. **Physical Plan** - як Spark виконає запит
2. **Оптимізації:**
   - Broadcast Join
   - Filter Pushdown
   - Two-Phase Aggregation
   - Partition Pruning
3. **Потенційні проблеми:**
   - Shuffle operations
   - SinglePartition exchanges
   - Large explode expansions

### Приклад читання explain():

```
Physical Plan:
├── TakeOrderedAndProject        # Top-N без повного сортування
│   └── BroadcastHashJoin        # JOIN через broadcast малої таблиці
│       ├── Filter               # Фільтр застосований рано
│       └── BroadcastExchange    # Таблиця розповсюджена на всі ноди
```

## Додаткові ресурси

- **BUSINESS_QUESTIONS.md** - Детальний аналіз кожного питання з explain()
- **data_loader.py** - Схеми та функції завантаження
- **ANALYSIS_REPORT.md** - Загальний аналіз датасету

## Troubleshooting

### OutOfMemoryError
```python
# Збільшити memory
spark.conf.set("spark.driver.memory", "8g")
# Або зменшити dataset фільтрацією
.filter(col("startYear") >= 2000)
```

### Занадто довге виконання
```python
# Додати limit для тестування
.limit(1000)
# Або використати sample
.sample(fraction=0.1)
```

### UnicodeEncodeError (Windows)
```python
# Вже виправлено в коді - використовуються [OK] замість емодзі
print("[OK] Успішно")  # Замість print("✅ Успішно")
```

## Контакти та питання

Для питань по реалізації або оптимізації запитів - дивіться детальний аналіз у `BUSINESS_QUESTIONS.md`.

---

**Примітка:** План виконання може відрізнятися залежно від версії Spark, розміру даних та конфігурації кластера. Наведені аналізи базуються на Spark 3.5+ з Catalyst optimizer.
