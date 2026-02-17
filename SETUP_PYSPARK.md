# Встановлення PySpark на локальному середовищі

**Автор:** Качмар Ігор  
**Етап:** Підготовка  
**Дата:** Лютий 2026

## Системні вимоги

- Python 3.8 або новіше
- Java 11 або новіше
- Windows 10/11 (64-bit)
- Мінімум 8GB RAM

## Крок 1: Встановлення Java

```bash
# Завантажити Java 11 JDK з офіційного сайту Oracle або AdoptOpenJDK
# Встановити та налаштувати змінну середовища JAVA_HOME
```

Перевірка встановлення:
```bash
java -version
```

## Крок 2: Встановлення Python

```bash
# Завантажити Python 3.8+ з python.org
# Встановити з опцією "Add Python to PATH"
```

Перевірка:
```bash
python --version
```

## Крок 3: Створення віртуального середовища

```bash
python -m venv .venv
```

Активація (Windows):
```bash
.venv\Scripts\activate
```

## Крок 4: Встановлення PySpark

```bash
pip install pyspark==3.5.0
```

## Крок 5: Перевірка встановлення

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Test") \
    .master("local[*]") \
    .getOrCreate()

print(spark.version)
spark.stop()
```

## Налаштування .gitignore

Створено `.gitignore` файл для виключення:
- Віртуальне середовище (.venv/)
- Python кеш (__pycache__/)
- PySpark метадані (metastore_db/, spark-warehouse/)
- Великі датасети (dataset/*.tsv)
- IDE файли (.idea/, .vscode/)

## GitHub Repository

- Створено репозиторій на GitHub
- Налаштовано права доступу для команди (5 учасників)
- Створено базову структуру проєкту

## Результат

✓ PySpark успішно встановлено  
✓ Java середовище налаштовано  
✓ Репозиторій створено  
✓ .gitignore налаштовано  

**Статус:** Завершено
