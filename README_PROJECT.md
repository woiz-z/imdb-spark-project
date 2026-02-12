# IMDb Data Analysis with PySpark

Проєкт для аналізу великих даних IMDb з використанням Apache Spark (PySpark).

## Статус виконання завдань

- [x] ✓ Попередній перегляд набору даних  
- [x] ✓ Створення схем для всіх наборів даних
- [x] ✓ Створення DataFrame з використанням схем
- [x] ✓ Перевірка коректності завантаження даних
- [x] ✓ Створення модуля data_loader.py з функціями завантаження

## Структура проєкту

```
BBD/
├── dataset/                   # TSV файли IMDb (9+ GB)
│   ├── name.basics.tsv       # 15M+ записів - люди
│   ├── title.basics.tsv      # 12M+ записів - фільми/серіали  
│   ├── title.ratings.tsv     # 1.6M+ записів - рейтинги
│   └── ...інші файли
├── data_loader.py            # Модуль завантаження даних
├── main.py                   # Головний файл
├── examples.py               # Приклади використання
├── requirements.txt          # Залежності Python
└── README.md                 # Цей файл
```

## Швидкий старт

### 1. Встановлення залежностей

```bash
pip install -r requirements.txt
```

### 2. Запуск основної програми

```bash
# Якщо використовуєте віртуальне середовище
E:/BBD/.venv/Scripts/python.exe main.py

# Або просто
python main.py
```

### 3. Перегляд прикладів

```bash
python examples.py
```

## Модуль data_loader.py

### Основні функції

```python
from data_loader import (
    load_title_basics,      # Основна інформація про фільми
    load_title_ratings,     # Рейтинги
    load_name_basics,       # Інформація про людей
    load_title_crew,        # Команда (режисери, сценаристи)
    load_title_principals,  # Головні учасники
    load_title_akas,        # Альтернативні назви
    load_title_episode      # Епізоди серіалів
)
```

### Приклад використання

```python
from pyspark.sql import SparkSession
from data_loader import load_title_basics, load_title_ratings

# Створення Spark сесії
spark = SparkSession.builder \
    .appName("IMDb") \
    .master("local[*]") \
    .getOrCreate()

# Завантаження даних з визначеними схемами
df_titles = load_title_basics(spark)
df_ratings = load_title_ratings(spark)

# Об'єднання та аналіз
df = df_titles.join(df_ratings, "tconst")
df.show()
```

## Результати тестування

### Завантажені набори даних:

| Файл | Записів | Статус |
|------|---------|--------|
| title.basics.tsv | 12,287,568 | OK |
| title.ratings.tsv | 1,634,950 | OK |
| name.basics.tsv | 15,085,440 | OK |
| title.crew.tsv | 12,287,568 | OK |

### Топ-10 фільмів IMDb:

1. **The Shawshank Redemption** (1994) - 9.3 - 3.1M голосів
2. **The Godfather** (1972) - 9.2 - 2.2M голосів  
3. **The Dark Knight** (2008) - 9.1 - 3.1M голосів
4. **The Godfather Part II** (1974) - 9.0 - 1.5M голосів
5. **Schindler's List** (1993) - 9.0 - 1.6M голосів

## Особливості реалізації

### Визначені схеми даних

Всі DataFrame створюються з явно визначеними схемами:
- Підвищує продуктивність
- Гарантує типобезпеку
- Полегшує відлагодження

### Обробка NULL значень

IMDb використовує `\N` для NULL - модуль автоматично обробляє це.

### Оптимізація для Windows

- Налаштовані змінні середовища `PYSPARK_PYTHON`
- Вимкнено Arrow для сумісності
- Оптимізовані параметри Spark

## Технічні вимоги

- Python 3.8+
- PySpark 3.5+
- Java 11+
- 8+ GB RAM (рекомендовано)
- Windows/Linux/macOS

## Документація

Детальна документація модуля data_loader.py: [DATA_LOADER_README.md](DATA_LOADER_README.md)

## Наступні кроки

- [ ] Додати більше методів аналізу
- [ ] Створити візуалізації
- [ ] Оптимізувати швидкість завантаження
- [ ] Додати кешування результатів

## Автор

BBD Project - 2026
