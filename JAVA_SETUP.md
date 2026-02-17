# Налаштування Java та змінних середовища

**Автор:** Яйко Назар  
**Етап:** Налаштування (Setup)  
**Дата:** Лютий 2026

## Встановлення Java 11

### Завантаження та встановлення

**Рекомендоване:** OpenJDK 11 (Adoptium)  
**Джерело:** https://adoptium.net/

**Windows:**
1. Завантажити інсталятор JDK 11 (.msi)
2. Встановити з опціями за замовчуванням
3. Шлях встановлення: `C:\Program Files\Eclipse Adoptium\jdk-11.0.22.7-hotspot\`

**Перевірка:**
```bash
java -version
# openjdk version "11.0.22" 2024-01-16
# OpenJDK Runtime Environment Temurin-11.0.22+7 (build 11.0.22+7)
# OpenJDK 64-Bit Server VM Temurin-11.0.22+7 (build 11.0.22+7, mixed mode)
```

## Налаштування змінних середовища

### Windows

**1. JAVA_HOME**

```bash
# Системні змінні → Нова змінна
Ім'я: JAVA_HOME
Значення: C:\Program Files\Eclipse Adoptium\jdk-11.0.22.7-hotspot
```

**2. PATH (додати Java)**

```bash
# Системні змінні → Path → Додати
%JAVA_HOME%\bin
```

**3. SPARK_HOME (опціонально для локального Spark)**

```bash
# Для Windows (якщо є локальний Spark)
SPARK_HOME=C:\spark-3.5.0-bin-hadoop3

# Додати до PATH
%SPARK_HOME%\bin
```

**4. PySpark змінні**

```bash
PYSPARK_PYTHON=E:\BBD\.venv\Scripts\python.exe
PYSPARK_DRIVER_PYTHON=E:\BBD\.venv\Scripts\python.exe
```

### Налаштування через PowerShell

```powershell
# Тимчасове налаштування (поточна сесія)
$env:JAVA_HOME = "C:\Program Files\Eclipse Adoptium\jdk-11.0.22.7-hotspot"
$env:PATH += ";$env:JAVA_HOME\bin"

# Перевірка
echo $env:JAVA_HOME
java -version
```

### Налаштування через .env файл (для проєкту)

```bash
Setup конфігураційний файл `.env` у корені проєкту:

# Java
JAVA_HOME=C:\Program Files\Eclipse Adoptium\jdk-11.0.22.7-hotspot

# Python/PySpark
PYSPARK_PYTHON=python
PYSPARK_DRIVER_PYTHON=python

# Spark (якщо локальний)
SPARK_LOCAL_DIRS=E:\BBD\spark-temp
```

## Тестування Docker

### docker run - базовий тест

```bash
# Запуск побудованого образу
docker run my-spark-img

# Очікуваний вивід:
# Spark version: 3.5.0
# (виконується main.py)
```

### docker run з інтерактивним режимом

```bash
# Зайти всередину контейнера
docker run -it my-spark-img bash

# Всередині контейнера тестуємо:
python --version
# Python 3.11.x

echo $JAVA_HOME
# /usr/lib/jvm/java-11-openjdk-amd64

java -version
# openjdk version "11.0.x"

python -c "import pyspark; print(pyspark.__version__)"
# 3.5.0
```

### docker run з volume (для датасетів)

```bash
# Запуск з прокиданням датасету
docker run -v E:/BBD/dataset:/app/dataset my-spark-img

# Тест читання файлу всередині контейнера
docker run -it -v E:/BBD/dataset:/app/dataset my-spark-img bash
ls -lh /app/dataset
```

### docker run з портами (Spark UI)

```bash
# Порт 4040 для Spark UI
docker run -p 4040:4040 my-spark-img

# Відкрити в браузері: http://localhost:4040
```

## Скрипт перевірки середовища

```bash
# test_environment.sh (або .bat для Windows)

echo "=== Перевірка Java ==="
java -version
echo ""

echo "=== Перевірка змінних середовища ==="
echo "JAVA_HOME=$JAVA_HOME"
echo "PYSPARK_PYTHON=$PYSPARK_PYTHON"
echo""

echo "=== Перевірка Python ==="
python --version
echo ""

echo "=== Перевірка PySpark ==="
python -c "import pyspark; print(f'PySpark: {pyspark.__version__}')"
echo ""

echo "=== Перевірка Spark сесії ==="
python -c "from pyspark.sql import SparkSession; spark = SparkSession.builder.appName('test').getOrCreate(); print(f'Spark: {spark.version}'); spark.stop()"
echo ""

echo "✓ Всі перевірки завершено!"
```

## Результати налаштування

### Змінні середовища

| Змінна | Значення | Статус |
|--------|----------|--------|
| JAVA_HOME | C:\Program Files\...\jdk-11.0.22.7-hotspot | ✓ OK |
| PATH | містить %JAVA_HOME%\bin | ✓ OK |
| PYSPARK_PYTHON | python / .venv\Scripts\python.exe | ✓ OK |

### Docker тести

| Тест | Команда | Результат |
|------|---------|-----------|
| Базовий запуск | `docker run my-spark-img` | ✓ OK |
| Інтерактивний | `docker run -it my-spark-img bash` | ✓ OK |
| З volume | `docker run -v .../dataset:/app/dataset` | ✓ OK |
| З портами | `docker run -p 4040:4040 my-spark-img` | ✓ OK |

## Висновки

✓ Java 11 встановлено та налаштовано  
✓ JAVA_HOME та PATH змінні налаштовані  
✓ PySpark змінні середовища встановлені  
✓ Docker run протестовано (базовий, інтерактивний, volume, порти)  
✓ Скрипт перевірки середовища створено  

**Статус:** Завершено
