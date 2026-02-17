# Налаштування Python середовища та IDE

**Автор:** Манюк Руслан  
**Етап:** Налаштування (Setup)  
**Дата:** Лютий 2026

## Налаштування Python 3.8+ середовища

### 1. Встановлення Python

**Версія:** Python 3.11.7 (рекомендовано 3.8-3.11)

**Завантаження:**
- Офіційний сайт: https://www.python.org/downloads/
- Важливо: ✓ "Add Python to PATH" під час встановлення

**Перевірка:**
```bash
python --version
# Python 3.11.7

python -m pip --version
# pip 24.0 або новіше
```

### 2. Створення віртуального середовища

```bash
# Перехід до директорії проєкту
cd E:\BBD

# Створення віртуального середовища
python -m venv .venv

# Активація (Windows)
.venv\Scripts\activate

# Активація (Linux/Mac)
source .venv/bin/activate
```

**Переваги віртуального середовища:**
- Ізоляція залежностей проєкту
- Не конфліктує з іншими Python проєктами
- Легко відтворити на іншому комп'ютері

### 3. Встановлення залежностей

```bash
# Оновлення pip
python -m pip install --upgrade pip

# Встановлення PySpark та інших залежностей
pip install pyspark

# Або встановлення всіх залежностей з requirements.txt
pip install -r requirements.txt
```

**Встановлені пакети:**
```
pyspark==3.5.0
pandas==2.2.0
numpy==1.26.4
pyarrow==15.0.0
```

**Перевірка встановлення:**
```bash
pip list | findstr pyspark
# pyspark     3.5.0

python -c "import pyspark; print(pyspark.__version__)"
# 3.5.0
```

## Встановлення PyCharm IDE

### 1. Завантаження PyCharm

**Версія:** PyCharm Community Edition (безкоштовна)  
**Завантаження:** https://www.jetbrains.com/pycharm/download/

**Альтернатива:** PyCharm Professional (платна, але більше функцій для Big Data)

### 2. Конфігурація PyCharm

#### Налаштування інтерпретатора

1. File → Settings → Project: BBD → Python Interpreter
2. Натиснути ⚙️ → Add
3. Вибрати "Existing environment"
4. Вказати шлях: `E:\BBD\.venv\Scripts\python.exe`
5. ✓ "Make available to all projects" (опціонально)

#### Налаштування для PySpark

**File → Settings → Project Structure:**
- Позначити `dataset/` як "Excluded" (щоб IDE не індексував великі файли)

**File → Settings → Editor → Code Style → Python:**
- Line length: 120
- ✓ Use tab character: false
- Tab size: 4

#### Корисні плагіни

```
1. Markdown Support - для README.md
2. CSV Plugin - для перегляду TSV файлів
3. .ignore - для .gitignore
```

**Встановлення плагінів:**
File → Settings → Plugins → Marketplace

### 3. Запуск коду в PyCharm

#### Варіант 1: Безпосередній запуск

```python
# main.py
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("IMDb Analysis") \
    .master("local[*]") \
    .getOrCreate()

print(f"Spark version: {spark.version}")
spark.stop()
```

**Запуск:** Right-click на `main.py` → Run 'main'

#### Варіант 2: Запуск через термінал

```bash
# Термінал всередині PyCharm (Alt+F12)
python main.py
```

## Тестування середовища

### Тест 1: PySpark імпорт

```python
# test_pyspark.py
try:
    import pyspark
    print(f"✓ PySpark {pyspark.__version__} встановлено")
except ImportError:
    print("❌ PySpark не встановлено")
```

### Тест 2: Створення Spark сесії

```python
# test_spark_session.py
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Setup Test") \
    .master("local[*]") \
    .config("spark.driver.memory", "2g") \
    .getOrCreate()

print(f"✓ Spark версія: {spark.version}")
print(f"✓ Spark master: {spark.sparkContext.master}")
print(f"✓ Доступних ядер: {spark.sparkContext.defaultParallelism}")

# Тест створення DataFrame
data = [("Alice", 25), ("Bob", 30), ("Charlie", 35)]
df = spark.createDataFrame(data, ["name", "age"])

print("\n✓ Тестовий DataFrame:")
df.show()

spark.stop()
print("\n✓ Spark сесію закрито")
```

**Очікуваний вивід:**
```
✓ Spark версія: 3.5.0
✓ Spark master: local[*]
✓ Доступних ядер: 8

✓ Тестовий DataFrame:
+-------+---+
|   name|age|
+-------+---+
|  Alice| 25|
|    Bob| 30|
|Charlie| 35|
+-------+---+

✓ Spark сесію закрито
```

### Тест 3: Читання TSV файлу

```python
# test_read_tsv.py
from pyspark.sql import SparkSession
from pathlib import Path

spark = SparkSession.builder \
    .appName("TSV Test") \
    .getOrCreate()

# Перевірка наявності файлу
tsv_file = "dataset/title.ratings.tsv"

if Path(tsv_file).exists():
    df = spark.read \
        .option("header", "true") \
        .option("delimiter", "\t") \
        .csv(tsv_file)
    
    print(f"✓ Файл завантажено: {df.count():,} записів")
    print(f"✓ Колонки: {df.columns}")
    df.show(5)
else:
    print(f"⚠️ Файл {tsv_file} не знайдено")

spark.stop()
```

## Результати налаштування

### Встановлене ПЗ

| Компонент | Версія | Статус |
|-----------|--------|--------|
| Python | 3.11.7 | ✓ OK |
| pip | 24.0 | ✓ OK |
| PySpark | 3.5.0 | ✓ OK |
| Pandas | 2.2.0 | ✓ OK |
| NumPy | 1.26.4 | ✓ OK |
| PyArrow | 15.0.0 | ✓ OK |
| PyCharm | Community 2024.1 | ✓ OK |

### Конфігурація середовища

```bash
# Змінні середовища (Windows)
JAVA_HOME=C:\Program Files\Java\jdk-11
PYSPARK_PYTHON=E:\BBD\.venv\Scripts\python.exe
PYSPARK_DRIVER_PYTHON=E:\BBD\.venv\Scripts\python.exe
```

### Структура віртуального середовища

```
.venv/
├── Scripts/
│   ├── python.exe          # Python інтерпретатор
│   ├── pip.exe             # Package manager
│   └── activate.bat        # Скрипт активації
├── Lib/
│   └── site-packages/      # Встановлені пакети
│       ├── pyspark/
│       ├── pandas/
│       ├── numpy/
│       └── ...
└── pyvenv.cfg              # Конфігурація venv
```

## Поширені проблеми та рішення

### Проблема 1: `python` не розпізнається

**Рішення:**
```bash
# Додати Python до PATH
# Windows: Системні змінні → Path → Додати → C:\Python311
```

### Проблема 2: `pip install pyspark` падає

**Рішення:**
```bash
# Оновити pip
python -m pip install --upgrade pip

# Встановити з явною версією
pip install pyspark==3.5.0
```

### Проблема 3: PyCharm не бачить пакети

**Рішення:**
- File → Invalidate Caches → Invalidate and Restart
- Перевірити інтерпретатор: Settings → Python Interpreter

## Висновки

✓ Python 3.11.7 встановлено та налаштовано  
✓ Віртуальне середовище `.venv` створено  
✓ PySpark 3.5.0 встановлено через `pip install pyspark`  
✓ PyCharm IDE налаштовано для роботи з проєктом  
✓ Всі тести середовища пройдені успішно  

**Статус:** Завершено  
**Середовище готове для розробки**
