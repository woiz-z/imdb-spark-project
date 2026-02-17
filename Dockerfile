# Базовий образ з Python 3.11
FROM python:3.11-slim

# Мета-дані
LABEL maintainer="Іванчук Орест"
LABEL description="PySpark IMDb Data Analysis Project"
LABEL version="1.0"

# Встановлення Java 11 (потрібен для PySpark)
RUN apt-get update && apt-get install -y \
    openjdk-11-jdk \
    curl \
    procps \
    && rm -rf /var/lib/apt/lists/*

# Налаштування JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

# Робоча директорія
WORKDIR /app

# Копіювання requirements.txt та встановлення залежностей
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Копіювання коду проєкту
COPY *.py ./
COPY *.md ./

# Налаштування Spark
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3

# Порт для Spark UI (опціонально)
EXPOSE 4040

# Команда за замовчуванням
CMD ["python", "main.py"]
