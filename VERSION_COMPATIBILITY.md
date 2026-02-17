# Координація підготовчого етапу та перевірка сумісності

**Автор:** Чігур Володимир  
**Етап:** Підготовка  
**Роль:** Координатор підготовчого етапу  
**Дата:** Лютий 2026

## Координація підготовчого етапу

### Розподіл завдань команди

| Учасник | Завдання | Статус |
|---------|----------|--------|
| Качмар Ігор | Встановлення PySpark, GitHub repo, .gitignore | ✓ Завершено |
| Іванчук Орест | Гілки Git, завантаження датасету, огляд даних | ✓ Завершено |
| Манюк Руслан | Аналіз типів даних, визначення корисних полів | ✓ Завершено |
| Яйко Назар | Структура директорій, валідація даних | ✓ Завершено |
| Чігур Володимир | requirements.txt, перевірка сумісності, координація | ✓ Завершено |

### Створені артефакти

1. **.gitignore** - конфігурація (Качмар І.)
2. **SETUP_PYSPARK.md** - інструкція встановлення (Качмар І.)
3. **DATASET_OVERVIEW.md** - огляд даних (Іванчук О.)
4. **DATA_TYPES_ANALYSIS.md** - аналіз типів (Манюк Р.)
5. **PROJECT_SETUP.md** - структура проєкту (Яйко Н.)
6. **validate_dataset.py** - скрипт валідації (Яйко Н.)
7. **requirements.txt** - залежності (Чігур В.)
8. **VERSION_COMPATIBILITY.md** - ця документація (Чігур В.)

## Перевірка сумісності версій інструментів

### Python залежності - requirements.txt

```txt
# PySpark - основна бібліотека для роботи з великими даними
pyspark>=3.5.0

# Pandas - для локального аналізу та конвертації
pandas>=2.2.0

# NumPy - математичні операції (обмежено <2.0 для сумісності)
numpy>=1.26.0,<2.0.0

# PyArrow - оптимізація передачі даних між Spark та Pandas
pyarrow>=15.0.0
```

### Перевірка версій

#### 1. Python

**Рекомендована версія:** 3.8 - 3.11  
**Причина:** PySpark 3.5.0 офіційно підтримує Python 3.8-3.11

```bash
python --version
# Очікується: Python 3.8.x або новіше
```

**Тестування:**
```python
import sys
print(f"Python версія: {sys.version}")

# Перевірка мінімальної версії
assert sys.version_info >= (3, 8), "Потрібен Python 3.8+"
print("✓ Python версія відповідає вимогам")
```

#### 2. Java

**Рекомендована версія:** Java 11 (LTS)  
**Сумісність:** Java 8, 11, 17  

```bash
java -version
# Очікується: openjdk version "11.0.x" або java version "11.0.x"
```

**Чому Java 11:**
- LTS (Long Term Support) до 2026+
- Офіційно підтримується PySpark 3.5.0
- Оптимальна продуктивність

#### 3. PySpark

**Версія:** 3.5.0+  
**Сумісність з компонентами:**

| Компонент | Версія | Сумісність |
|-----------|--------|------------|
| Python | 3.8-3.11 | ✓ Повна |
| Java | 11 | ✓ Рекомендовано |
| Pandas | 2.2.0+ | ✓ Повна |
| NumPy | 1.26.0+ | ⚠️ <2.0.0 |
| PyArrow | 15.0.0+ | ✓ Повна |

**Перевірка встановлення:**
```python
import pyspark
print(f"PySpark версія: {pyspark.__version__}")

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("test").getOrCreate()
print(f"Spark версія: {spark.version}")
spark.stop()
```

#### 4. Pandas

**Версія:** 2.2.0+  
**Використання:** Конвертація Spark DataFrame ↔ Pandas DataFrame

```python
import pandas as pd
print(f"Pandas версія: {pd.__version__}")
```

#### 5. NumPy

**Версія:** 1.26.0 - 1.26.x (NOT 2.0+)  
**Важливо:** NumPy 2.0 має breaking changes, не сумісні з PySpark 3.5.0

```python
import numpy as np
print(f"NumPy версія: {np.__version__}")

# Перевірка обмеження версії
assert np.__version__.startswith('1.'), "NumPy повинен бути версії 1.x"
print("✓ NumPy версія коректна")
```

#### 6. PyArrow

**Версія:** 15.0.0+  
**Використання:** Оптимізація toPandas() та createDataFrame()

```python
import pyarrow as pa
print(f"PyArrow версія: {pa.__version__}")
```

### Скрипт автоматичної перевірки

```python
"""
Автоматична перевірка сумісності версій
Автор: Чігур Володимир
"""

def check_compatibility():
    """Перевірка всіх залежностей та їх сумісності"""
    
    print("="*60)
    print("ПЕРЕВІРКА СУМІСНОСТІ ВЕРСІЙ")
    print("="*60 + "\n")
    
    errors = []
    warnings = []
    
    # 1. Python
    import sys
    python_version = f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}"
    print(f"Python: {python_version}")
    
    if sys.version_info < (3, 8):
        errors.append("Python версія < 3.8")
    elif sys.version_info >= (3, 12):
        warnings.append("Python версія >= 3.12 (не тестована з PySpark 3.5)")
    else:
        print("  ✓ Версія підтримується")
    
    # 2. PySpark
    try:
        import pyspark
        pyspark_version = pyspark.__version__
        print(f"\nPySpark: {pyspark_version}")
        
        if pyspark_version < "3.5.0":
            errors.append("PySpark версія < 3.5.0")
        else:
            print("  ✓ Версія підтримується")
    except ImportError:
        errors.append("PySpark не встановлено")
    
    # 3. Pandas
    try:
        import pandas as pd
        pandas_version = pd.__version__
        print(f"\nPandas: {pandas_version}")
        
        if pd.__version__ < "2.2.0":
            warnings.append("Pandas версія < 2.2.0 (рекомендується оновлення)")
        else:
            print("  ✓ Версія підтримується")
    except ImportError:
        errors.append("Pandas не встановлено")
    
    # 4. NumPy
    try:
        import numpy as np
        numpy_version = np.__version__
        print(f"\nNumPy: {numpy_version}")
        
        if np.__version__.startswith('2.'):
            errors.append("NumPy версія 2.x не сумісна з PySpark 3.5")
        elif np.__version__ < "1.26.0":
            warnings.append("NumPy версія < 1.26.0")
        else:
            print("  ✓ Версія підтримується")
    except ImportError:
        errors.append("NumPy не встановлено")
    
    # 5. PyArrow
    try:
        import pyarrow as pa
        pyarrow_version = pa.__version__
        print(f"\nPyArrow: {pyarrow_version}")
        
        if pa.__version__ < "15.0.0":
            warnings.append("PyArrow версія < 15.0.0")
        else:
            print("  ✓ Версія підтримується")
    except ImportError:
        warnings.append("PyArrow не встановлено (опціонально, але рекомендується)")
    
    # 6. Java (через PySpark)
    try:
        from pyspark.sql import SparkSession
        spark = SparkSession.builder \
            .appName("Version Check") \
            .master("local[1]") \
            .getOrCreate()
        
        java_version = spark.sparkContext._gateway.jvm.System.getProperty("java.version")
        print(f"\nJava: {java_version}")
        
        if java_version.startswith("1.8") or java_version.startswith("11") or java_version.startswith("17"):
            print("  ✓ Версія підтримується")
        else:
            warnings.append(f"Java версія {java_version} не тестована")
        
        spark.stop()
    except Exception as e:
        errors.append(f"Не вдалося перевірити Java: {e}")
    
    # Результат
    print("\n" + "="*60)
    
    if errors:
        print("❌ КРИТИЧНІ ПОМИЛКИ:")
        for error in errors:
            print(f"  - {error}")
    
    if warnings:
        print("\n⚠️  ПОПЕРЕДЖЕННЯ:")
        for warning in warnings:
            print(f"  - {warning}")
    
    if not errors and not warnings:
        print("✓ ВСІ ВЕРСІЇ СУМІСНІ!")
    elif not errors:
        print("\n✓ Немає критичних помилок, але є попередження")
    else:
        print("\n❌ Виявлено критичні помилки сумісності")
    
    print("="*60)
    
    return len(errors) == 0

if __name__ == "__main__":
    check_compatibility()
```

### Результати тестування сумісності

**Тестове середовище:**
- Windows 11 (64-bit)
- Python 3.11.7
- Java 11.0.22
- 16 GB RAM

**Результати:**

| Пакет | Версія | Статус |
|-------|--------|--------|
| Python | 3.11.7 | ✓ OK |
| Java | 11.0.22 | ✓ OK |
| PySpark | 3.5.0 | ✓ OK |
| Pandas | 2.2.0 | ✓ OK |
| NumPy | 1.26.4 | ✓ OK |
| PyArrow | 15.0.0 | ✓ OK |

**Висновок:** Всі версії сумісні між собою ✓

## Інструкція встановлення

### Крок 1: Встановлення базових інструментів

```bash
# Python 3.8-3.11
# Завантажити з python.org

# Java 11
# Завантажити з adoptium.net або oracle.com
```

### Крок 2: Створення віртуального середовища

```bash
python -m venv .venv
.venv\Scripts\activate  # Windows
# або
source .venv/bin/activate  # Linux/Mac
```

### Крок 3: Встановлення Python пакетів

```bash
pip install -r requirements.txt
```

### Крок 4: Перевірка установки

```bash
python check_compatibility.py
```

## Висновки

✓ requirements.txt створено з правильними версіями  
✓ Всі залежності перевірені на сумісність  
✓ Скрипт автоматичної перевірки підготовлено  
✓ Підготовчий етап скоординовано успішно  

**Команда готова до наступного етапу: Налаштування (Setup)**

**Статус:** Завершено
