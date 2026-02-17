# Аналіз типів даних у датасеті IMDb

**Автор:** Манюк Руслан  
**Етап:** Підготовка  
**Дата:** Лютий 2026

## Аналіз типів даних

### 1. title.basics.tsv (12,287,568 записів)

| Поле | Тип даних | Приклад | Корисність | Примітки |
|------|-----------|---------|------------|----------|
| `tconst` | String (ID) | tt0000001 | ✓ Критичне | PRIMARY KEY |
| `titleType` | String (category) | movie, tvSeries | ✓ Висока | Фільтрація типів |
| `primaryTitle` | String | The Shawshank Redemption | ✓ Висока | Основна назва |
| `originalTitle` | String | The Shawshank Redemption | ~ Середня | Дублікат для EN фільмів |
| `isAdult` | Boolean (0/1) | 0 | ✓ Висока | Фільтрація контенту |
| `startYear` | Integer | 1994 | ✓ Критична | Аналіз трендів |
| `endYear` | Integer/NULL | \N | ~ Низька | Тільки для серіалів |
| `runtimeMinutes` | Integer | 142 | ✓ Висока | Аналіз тривалості |
| `genres` | Array[String] | Drama,Crime | ✓ Критична | Багатозначне поле |

**Рекомендації:**
- `tconst` → StringType (ключ для JOIN)
- `startYear` → IntegerType (для фільтрації >2000)
- `runtimeMinutes` → IntegerType (для аналізу)
- `genres` → StringType → split по `,`
- `endYear` → можна ігнорувати (95% NULL)

### 2. title.ratings.tsv (1,634,950 записів)

| Поле | Тип даних | Діапазон | Корисність | Примітки |
|------|-----------|----------|------------|----------|
| `tconst` | String (ID) | tt0000001 | ✓ Критичне | FOREIGN KEY |
| `averageRating` | Float | 1.0 - 10.0 | ✓ Критична | Основна метрика |
| `numVotes` | Integer | 5 - 3M+ | ✓ Критична | Вага рейтингу |

**Рекомендації:**
- `averageRating` → DoubleType (для точності)
- `numVotes` → IntegerType
- Фільтрувати `numVotes > 1000` (надійні рейтинги)

### 3. name.basics.tsv (15,085,440 записів)

| Поле | Тип даних | Приклад | Корисність | Примітки |
|------|-----------|---------|------------|----------|
| `nconst` | String (ID) | nm0000001 | ✓ Критичне | PRIMARY KEY |
| `primaryName` | String | Morgan Freeman | ✓ Висока | Ім'я актора/режисера |
| `birthYear` | Integer/NULL | 1937 | ~ Середня | 30% NULL |
| `deathYear` | Integer/NULL | \N | ~ Низька | 95% NULL |
| `primaryProfession` | Array[String] | actor,director | ✓ Висока | Роль людини |
| `knownForTitles` | Array[String] | tt0111161,tt0137523 | ✓ Критична | Зв'язок з фільмами |

**Рекомендації:**
- `knownForTitles` → split по `,` → JOIN з title.basics
- `birthYear`, `deathYear` → IntegerType або NULL

### 4. title.crew.tsv (12,287,568 записів)

| Поле | Тип даних | Приклад | Корисність | Примітки |
|------|-----------|---------|------------|----------|
| `tconst` | String (ID) | tt0000001 | ✓ Критичне | FOREIGN KEY |
| `directors` | Array[String] | nm0000233 | ✓ Висока | Багатозначне |
| `writers` | Array[String] | nm0000233,nm0000231 | ~ Середня | Багатозначне |

**Рекомендації:**
- Обидва поля → split по `,`
- JOIN з name.basics для імен

### 5. title.principals.tsv (60M+ записів) ⚠️

| Поле | Тип даних | Корисність | Примітки |
|------|-----------|------------|----------|
| `tconst` | String | ✓ Висока | Зв'язок з фільмами |
| `ordering` | Integer | ~ Низька | Порядок акторів |
| `nconst` | String | ✓ Висока | Зв'язок з людьми |
| `category` | String | ✓ Висока | actor, director, producer |
| `job` | String | ~ Середня | Деталі ролі |
| `characters` | JSON String | ~ Середня | Ім'я персонажа |

**Проблема:** Файл ДУЖЕ великий (60M+ записів)  
**Рекомендація:** Використовувати тільки для топ-1000 фільмів

### 6. title.akas.tsv (40M+ записів) ⚠️

Альтернативні назви фільмів на різних мовах.

**Корисність:** Низька для англомовного аналізу  
**Рекомендація:** Пропустити або використати тільки `region='US'`

### 7. title.episode.tsv (8M+ записів)

Інформація про епізоди серіалів.

**Корисність:** Середня (тільки для аналізу серіалів)  
**Рекомендація:** Використати для TOP серіалів

## Визначення корисних полів

### ✅ Критично важливі

- `title.basics.tconst` - ключ
- `title.basics.primaryTitle` - назва
- `title.basics.startYear` - рік
- `title.basics.genres` - жанри
- `title.ratings.averageRating` - рейтинг
- `title.ratings.numVotes` - голоси
- `name.basics.primaryName` - ім'я
- `title.crew.directors` - режисери

### ⚠️ Корисні, але потребують обробки

- `title.basics.runtimeMinutes` - багато NULL
- `name.basics.birthYear` - 30% NULL
- `title.principals` - надто великий файл
- `title.basics.originalTitle` - дублікат

### ❌ Неінформативні / низька цінність

- `title.basics.endYear` - 95% NULL
- `name.basics.deathYear` - 95% NULL
- `title.akas` - надлишкова інформація
- `title.principals.ordering` - несуттєво
- `title.principals.job` - часто пусте

## NULL значення (`\N`)

### Статистика NULL по таблицях:

| Таблиця | Поле | % NULL |
|---------|------|--------|
| title.basics | endYear | 95% |
| title.basics | runtimeMinutes | 35% |
| title.crew | directors | 20% |
| title.crew | writers | 40% |
| name.basics | birthYear | 30% |
| name.basics | deathYear | 95% |

**Стратегія обробки:**
1. `\N` → NULL в PySpark
2. Фільтрувати записи з NULL в критичних полях
3. Використати `.na.drop()` або `.fillna()`

## Висновки

### Рекомендований набір для аналізу:

1. **title.basics** - всі записи, фільтр `titleType IN ('movie', 'tvMovie')`
2. **title.ratings** - всі записи, фільтр `numVotes > 1000`
3. **name.basics** - всі записи
4. **title.crew** - всі записи
5. **title.principals** - ⚠️ тільки TOP-1000 фільмів
6. **title.akas** - ❌ пропустити
7. **title.episode** - тільки для аналізу серіалів

### Типи даних для PySpark схем:

```python
from pyspark.sql.types import *

# title.basics
StringType()    # tconst, titleType, titles, genres
IntegerType()   # startYear, runtimeMinutes
BooleanTypeя()  # isAdult

# title.ratings
StringType()    # tconst
DoubleType()    # averageRating
IntegerType()   # numVotes

# name.basics
StringType()    # nconst, primaryName, professions, knownForTitles
IntegerType()   # birthYear, deathYear
```

**Статус:** Завершено  
**Наступний крок:** Створення PySpark схем
