# IMDb Data Analysis Project

## Опис проєкту

Проєкт для аналізу даних IMDb з використанням Apache Spark (PySpark).

## Структура проєкту

```
BBD/
├── dataset/                    # Директорія з TSV файлами IMDb
│   ├── name.basics.tsv        # Інформація про людей (15M+ записів)
│   ├── title.basics.tsv       # Основна інформація про фільми (12M+ записів)
│   ├── title.ratings.tsv      # Рейтинги (1.6M+ записів)
│   ├── title.crew.tsv         # Команда (режисери, сценаристи)
│   ├── title.principals.tsv   # Головні учасники
│   ├── title.akas.tsv         # Альтернативні назви
│   └── title.episode.tsv      # Епізоди серіалів
├── data_loader.py             # Модуль для завантаження даних
├── main.py                    # Головний файл програми
└── README.md                  # Ця документація
```

## Модуль data_loader.py

### Схеми даних

Модуль містить визначення схем для всіх наборів даних IMDb:

- `get_name_basics_schema()` - схема для інформації про людей
- `get_title_basics_schema()` - схема для фільмів/серіалів  
- `get_title_ratings_schema()` - схема для рейтингів
- `get_title_crew_schema()` - схема для команди
- `get_title_principals_schema()` - схема для головних учасників
- `get_title_akas_schema()` - схема для альтернативних назв
- `get_title_episode_schema()` - схема для епізодів

### Функції завантаження

Кожен набір даних має окрему функцію завантаження:

```python
from data_loader import load_title_basics, load_title_ratings

# Завантаження основної інформації про фільми
df_titles = load_title_basics(spark)

# Завантаження рейтингів
df_ratings = load_title_ratings(spark)
```

### Базова функція

`load_tsv_with_schema()` - універсальна функція для завантаження TSV файлів з визначеною схемою:

```python
from data_loader import load_tsv_with_schema, get_title_basics_schema

schema = get_title_basics_schema()
df = load_tsv_with_schema(spark, "dataset/title.basics.tsv", schema)
```

### Функція валідації

`validate_dataframe()` - перевіряє коректність завантаження DataFrame:

```python
from data_loader import validate_dataframe

validate_dataframe(df, "title.basics")
```

## Встановлення та запуск

### 1. Створення віртуального середовища

```bash
python -m venv .venv
.venv\Scripts\activate
```

### 2. Встановлення залежностей

```bash
pip install pyspark pandas numpy pyarrow
```

### 3. Запуск програми

```bash
# Використання віртуального середовища
E:/BBD/.venv/Scripts/python.exe main.py
```

## Результати тестування

### Завантажені набори даних:

- ✓ **title.basics**: 12,287,568 записів
- ✓ **title.ratings**: 1,634,950 записів  
- ✓ **name.basics**: 15,085,440 записів
- ✓ **title.crew**: 12,287,568 записів

### Приклад результатів:

**Топ-10 фільмів з найвищим рейтингом (мінімум 100,000 голосів):**

1. The Shawshank Redemption (1994) - 9.3 ⭐
2. The Godfather (1972) - 9.2 ⭐
3. The Dark Knight (2008) - 9.1 ⭐
4. The Godfather Part II (1974) - 9.0 ⭐
5. Schindler's List (1993) - 9.0 ⭐

## Основні можливості

### Схеми даних

Всі схеми визначені явно для:
- Коректної обробки типів даних
- Підвищення продуктивності
- Уникнення помилок при обробці

### Обробка NULL значень

IMDb використовує `\N` для позначення відсутніх значень. Модуль автоматично обробляє їх як NULL.

### Приклад використання

```python
from pyspark.sql import SparkSession
from data_loader import load_title_basics, load_title_ratings

# Створення Spark сесії
spark = SparkSession.builder \
    .appName("IMDb Analysis") \
    .master("local[*]") \
    .getOrCreate()

# Завантаження даних
df_titles = load_title_basics(spark)
df_ratings = load_title_ratings(spark)

# Об'єднання даних
df_combined = df_titles.join(df_ratings, "tconst", "left")

# Аналіз
top_movies = df_combined \
    .filter((df_combined.titleType == "movie") & 
            (df_combined.numVotes >= 100000)) \
    .orderBy(df_combined.averageRating.desc()) \
    .select("primaryTitle", "startYear", "averageRating", "numVotes") \
    .limit(10)

top_movies.show(truncate=False)
```

## Вимоги

- Python 3.8+
- PySpark 3.0+
- Java 11+
- Windows/Linux/macOS

## Автор

BBD Project Team - 2026
