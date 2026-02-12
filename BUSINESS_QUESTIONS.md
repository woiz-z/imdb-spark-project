# Бізнес-питання для аналізу IMDb даних

## Загальна інформація

**Команда:** 5 осіб  
**Всього питань:** 30 (6 питань на особу)  
**Дата:** Лютий 2026

### Вимоги до питань (на кожну особу):
- ✅ Мінімум 3 питання з `filter`
- ✅ Мінімум 2 питання з `join`
- ✅ Мінімум 2 питання з `group by`
- ✅ Мінімум 2 питання з `window functions`

---

## ОСОБА 1: Аналіз найкращих фільмів за різними критеріями

### Питання 1.1: ТОП-10 найрейтинговіших фільмів з мінімум 10000 голосів

**Бізнес-ціль:** Визначити найкращі фільми за рейтингом з достатньою кількістю голосів для надійності оцінки.

**Застосовані операції:**
- `FILTER`: titleType == "movie", isAdult == 0, numVotes >= 10000
- `JOIN`: title.basics + title.ratings
- `ORDER BY`: averageRating DESC, numVotes DESC
- `LIMIT`: 10

**PySpark код:**
```python
q1_1 = df_basics \
    .filter((col("titleType") == "movie") & (col("isAdult") == 0)) \
    .join(df_ratings, "tconst") \
    .filter(col("numVotes") >= 10000) \
    .select("tconst", "primaryTitle", "startYear", "genres", "averageRating", "numVotes") \
    .orderBy(col("averageRating").desc(), col("numVotes").desc()) \
    .limit(10)
```

**Аналіз плану виконання (explain()):**

```
Physical Plan:
├── TakeOrderedAndProject [limit=10, orderBy=[averageRating DESC, numVotes DESC]]
│   └── Project [tconst, primaryTitle, startYear, genres, averageRating, numVotes]
│       └── BroadcastHashJoin [tconst], Inner
│           ├── Filter [titleType = movie AND isAdult = 0]
│           │   └── FileScan csv [dataset/title.basics.tsv]
│           └── BroadcastExchange
│               └── Filter [numVotes >= 10000]
│                   └── FileScan csv [dataset/title.ratings.tsv]
```

**Оптимізації:**
1. **Filter Pushdown**: Фільтри застосовуються перед JOIN, зменшуючи обсяг даних
2. **Broadcast Join**: Таблиця ratings (меншого розміру) broadcast-иться на всі ноди
3. **TakeOrderedAndProject**: Оптимізована операція для top-N запитів без повного сортування
4. **Early Filter**: numVotes >= 10000 застосовується до JOIN, фільтруючи ~87% рейтингів

**Очікувана продуктивність:** Висока (2-5 сек на 1.6M рейтингів)

---

### Питання 1.2: Кількість фільмів по жанрах з середнім рейтингом

**Бізнес-ціль:** Визначити найпопулярніші та якісні жанри для інвестиційних рішень.

**Застосовані операції:**
- `FILTER`: titleType, isAdult, numVotes >= 1000, movie_count >= 100
- `JOIN`: title.basics + title.ratings
- `GROUP BY`: genre
- `EXPLODE`: розпакування масиву жанрів
- `AGGREGATION`: count, avg, sum

**PySpark код:**
```python
q1_2 = df_basics \
    .filter((col("titleType") == "movie") & (col("isAdult") == 0)) \
    .join(df_ratings, "tconst") \
    .filter(col("numVotes") >= 1000) \
    .withColumn("genre", explode(split(col("genres"), ","))) \
    .groupBy("genre") \
    .agg(
        count("*").alias("movie_count"),
        round(avg("averageRating"), 2).alias("avg_rating"),
        sum("numVotes").alias("total_votes")
    ) \
    .filter(col("movie_count") >= 100) \
    .orderBy(col("avg_rating").desc())
```

**Аналіз плану виконання:**

```
Physical Plan:
├── Sort [avg_rating DESC]
│   └── Filter [movie_count >= 100]
│       └── HashAggregate [groupBy=genre, agg={count, avg, sum}]
│           └── Exchange [HashPartitioning(genre)]
│               └── HashAggregate [groupBy=genre, partial agg]
│                   └── Generate [explode(split(genres, ','))]
│                       └── Project
│                           └── BroadcastHashJoin
│                               ├── Filter [title.basics conditions]
│                               └── Filter [ratings conditions]
```

**Оптимізації:**
1. **Two-Phase Aggregation**: Partial aggregate локально, потім final aggregate
2. **Hash Partitioning**: Дані перерозподіляються по ключу genre для ефективної агрегації
3. **Generate (Explode)**: Виконується після JOIN, щоб зменшити кількість даних
4. **Post-Aggregation Filter**: movie_count >= 100 застосовується після GROUP BY

**Потенційні проблеми:**
- Explode збільшує кількість рядків (фільми з 3 жанрами → 3 рядки)
- Shuffle operation для GROUP BY може бути costly

**Очікувана продуктивність:** Середня (10-20 сек через explode та shuffle)

---

### Питання 1.3: Фільми доступні українською мовою з високим рейтингом

**Бізнес-ціль:** Знайти якісний контент для українського ринку.

**Застосовані операції:**
- `FILTER`: region == "UA" OR language == "uk", titleType, isAdult, averageRating >= 7.0
- `JOIN`: title.akas + title.basics + title.ratings (3 таблиці)

**PySpark код:**
```python
q1_3 = df_akas \
    .filter((col("region") == "UA") | (col("language") == "uk")) \
    .join(df_basics, df_akas.titleId == df_basics.tconst) \
    .filter((col("titleType") == "movie") & (col("isAdult") == 0)) \
    .join(df_ratings, "tconst") \
    .filter(col("averageRating") >= 7.0) \
    .select(...) \
    .orderBy(col("averageRating").desc()) \
    .limit(20)
```

**Аналіз плану виконання:**

```
Physical Plan:
├── TakeOrderedAndProject [limit=20]
│   └── Project [selected columns]
│       └── BroadcastHashJoin [tconst], Inner
│           ├── Filter [averageRating >= 7.0]
│           │   └── FileScan [title.ratings.tsv]
│           └── BroadcastHashJoin [tconst], Inner
│               ├── Filter [titleType, isAdult]
│               │   └── FileScan [title.basics.tsv]
│               └── BroadcastExchange
│                   └── Filter [region=UA OR language=uk]
│                       └── FileScan [title.akas.tsv]
```

**Оптимізації:**
1. **Multiple Broadcast Joins**: Smaller tables (akas filtered, ratings) broadcast-яться
2. **Filter Pushdown**: Всі фільтри застосовуються перед JOIN
3. **Join Reordering**: Catalyst optimizer може змінити порядок JOIN для оптимальності
4. **Predicate Pushdown**: OR condition (region|language) може використати partition pruning

**Очікувана продуктивність:** Висока (3-7 сек, оскільки UA фільтр сильно зменшує akas table)

---

### Питання 1.4: Ранжування фільмів по рейтингу в кожному десятилітті

**Бізнес-ціль:** Визначити найкращі фільми кожної епохи для ретроспективних добірок.

**Застосовані операції:**
- `FILTER`: titleType, isAdult, startYear IS NOT NULL, numVotes >= 5000, rank <= 5
- `JOIN`: title.basics + title.ratings
- `WINDOW FUNCTION`: row_number() OVER (PARTITION BY decade ORDER BY rating DESC)

**PySpark код:**
```python
q1_4 = df_basics \
    .filter((col("titleType") == "movie") & (col("isAdult") == 0) & col("startYear").isNotNull()) \
    .join(df_ratings, "tconst") \
    .filter(col("numVotes") >= 5000) \
    .withColumn("decade", (floor(col("startYear") / 10) * 10).cast("int")) \
    .withColumn(
        "rank_in_decade",
        row_number().over(
            Window.partitionBy("decade")
            .orderBy(col("averageRating").desc(), col("numVotes").desc())
        )
    ) \
    .filter(col("rank_in_decade") <= 5) \
    .select(...)
    .orderBy("decade", "rank_in_decade")
```

**Аналіз плану виконання:**

```
Physical Plan:
├── Sort [decade ASC, rank_in_decade ASC]
│   └── Filter [rank_in_decade <= 5]
│       └── Window [row_number() OVER (PARTITION BY decade ORDER BY ...)]
│           └── Sort [decade ASC, averageRating DESC, numVotes DESC]
│               └── Exchange [HashPartitioning(decade)]
│                   └── Project [add decade column]
│                       └── BroadcastHashJoin
│                           ├── Filter [basics conditions]
│                           └── Filter [numVotes >= 5000]
```

**Оптимізації:**
1. **Window Function Optimization**: row_number() - ефективна window function
2. **Partition-wise Sort**: Сортування в межах кожної партиції (decade)
3. **Post-Window Filter**: rank <= 5 зменшує фінальний результат
4. **Hash Partitioning**: Дані перерозподіляються по decade для window operation

**Особливості:**
- **Double Sort**: Один для window, один для фінального результату
- **Shuffle Required**: Exchange operation для партиціонування по decade
- **Memory Usage**: Window operation зберігає дані партиції в пам'яті

**Очікувана продуктивність:** Середня (15-25 сек через window function і shuffle)

---

### Питання 1.5: Різниця рейтингу фільму з середнім рейтингом його жанру

**Бізнес-ціль:** Знайти фільми, які виділяються на фоні свого жанру.

**Застосовані операції:**
- `FILTER`: titleType, isAdult, numVotes >= 1000, rating_diff >= 1.5
- `JOIN`: title.basics + title.ratings
- `WINDOW FUNCTION`: avg() OVER (PARTITION BY genre)
- `EXPLODE`: розпакування жанрів

**PySpark код:**
```python
q1_5 = df_basics \
    .filter((col("titleType") == "movie") & (col("isAdult") == 0)) \
    .join(df_ratings, "tconst") \
    .filter(col("numVotes") >= 1000) \
    .withColumn("genre", explode(split(col("genres"), ","))) \
    .withColumn(
        "genre_avg_rating",
        avg("averageRating").over(Window.partitionBy("genre"))
    ) \
    .withColumn("rating_diff", round(col("averageRating") - col("genre_avg_rating"), 2)) \
    .filter(col("rating_diff") >= 1.5) \
    .select(...) \
    .orderBy(col("rating_diff").desc()) \
    .limit(20)
```

**Аналіз плану виконання:**

```
Physical Plan:
├── TakeOrderedAndProject [limit=20, orderBy=rating_diff DESC]
│   └── Filter [rating_diff >= 1.5]
│       └── Project [calculate rating_diff]
│           └── Window [avg(averageRating) OVER (PARTITION BY genre)]
│               └── Sort [genre ASC]
│                   └── Exchange [HashPartitioning(genre)]
│                       └── Generate [explode(genres)]
│                           └── BroadcastHashJoin
│                               ├── Filter [basics]
│                               └── Filter [ratings]
```

**Оптимізації:**
1. **Aggregating Window Function**: avg() обчислюється ефективно
2. **Explode Before Window**: Створює більше рядків, але дозволяє window по genre
3. **Filter After Window**: rating_diff >= 1.5 застосовується після обчислення

**Потенційні проблеми:**
- **Data Explosion**: Explode збільшує dataset в ~2-3 рази (середня кількість жанрів)
- **Shuffle Cost**: Exchange для партиціонування по genre
- **Memory**: Window operation потребує зберігання даних партиції

**Очікувана продуктивність:** Низька (25-40 сек через explode + window + shuffle)

---

### Питання 1.6: Фільми 2020-х років з найбільшою динамікою популярності

**Бізнес-ціль:** Аналіз трендів сучасного кіно для прогнозування популярності.

**Застосовані операції:**
- `FILTER`: titleType, isAdult, startYear BETWEEN 2020 AND 2025
- `JOIN`: title.basics + title.ratings
- `GROUP BY`: startYear
- `AGGREGATION`: count, avg, custom popularity_score

**PySpark код:**
```python
q1_6 = df_basics \
    .filter(
        (col("titleType") == "movie") & 
        (col("isAdult") == 0) & 
        (col("startYear") >= 2020) & 
        (col("startYear") <= 2025)
    ) \
    .join(df_ratings, "tconst") \
    .withColumn("popularity_score", col("averageRating") * log10(col("numVotes") + 1)) \
    .groupBy("startYear") \
    .agg(
        count("*").alias("movies_count"),
        round(avg("averageRating"), 2).alias("avg_rating"),
        round(avg("popularity_score"), 2).alias("avg_popularity")
    ) \
    .orderBy("startYear")
```

**Аналіз плану виконання:**

```
Physical Plan:
├── Sort [startYear ASC]
│   └── HashAggregate [groupBy=startYear, final agg]
│       └── Exchange [HashPartitioning(startYear)]
│           └── HashAggregate [groupBy=startYear, partial agg]
│               └── Project [calculate popularity_score]
│                   └── BroadcastHashJoin
│                       ├── Filter [startYear between 2020 and 2025]
│                       └── BroadcastExchange [ratings]
```

**Оптимізації:**
1. **Narrow Range Filter**: startYear 2020-2025 сильно зменшує dataset
2. **Two-Phase Aggregation**: Partial + Final для ефективності
3. **Small Cardinality**: GROUP BY має тільки 6 значень (2020-2025)
4. **Expression Evaluation**: popularity_score обчислюється перед aggregation

**Очікувана продуктивність:** Висока (3-5 сек через вузький діапазон років)

---

## ОСОБА 2: Аналіз акторів та режисерів

### Питання 2.1: ТОП-10 режисерів з найбільшою кількістю високорейтингових фільмів

**Бізнес-ціль:** Визначити найуспішніших режисерів для співпраці.

**Застосовані операції:**
- `FILTER`: directors IS NOT NULL, titleType, isAdult, averageRating >= 7.5, high_rated_movies >= 3
- `JOIN`: title.crew + title.basics + title.ratings + name.basics (4 таблиці!)
- `GROUP BY`: director_id, primaryName
- `EXPLODE`: розпакування списку режисерів

**PySpark код:**
```python
q2_1 = df_crew \
    .filter(col("directors").isNotNull()) \
    .withColumn("director_id", explode(split(col("directors"), ","))) \
    .join(df_basics, "tconst") \
    .filter((col("titleType") == "movie") & (col("isAdult") == 0)) \
    .join(df_ratings, "tconst") \
    .filter(col("averageRating") >= 7.5) \
    .join(df_names, df_crew.director_id == df_names.nconst) \
    .groupBy("director_id", "primaryName") \
    .agg(
        count("*").alias("high_rated_movies"),
        round(avg("averageRating"), 2).alias("avg_rating"),
        sum("numVotes").alias("total_votes")
    ) \
    .filter(col("high_rated_movies") >= 3) \
    .orderBy(col("avg_rating").desc(), col("high_rated_movies").desc()) \
    .limit(10)
```

**Аналіз плану виконання:**

```
Physical Plan:
├── TakeOrderedAndProject [limit=10]
│   └── Filter [high_rated_movies >= 3]
│       └── HashAggregate [groupBy=(director_id, primaryName), final]
│           └── Exchange [HashPartitioning(director_id, primaryName)]
│               └── HashAggregate [partial]
│                   └── BroadcastHashJoin [director_id = nconst]
│                       ├── BroadcastExchange [names table]
│                       └── BroadcastHashJoin [tconst]
│                           ├── Filter [averageRating >= 7.5]
│                           └── BroadcastHashJoin [tconst]
│                               ├── Filter [basics conditions]
│                               └── Generate [explode directors]
│                                   └── Filter [directors IS NOT NULL]
```

**Оптимізації:**
1. **Multiple Broadcast Joins**: Всі невеликі таблиці broadcast-яться
2. **Filter Before Explode**: Фільтрація NULL перед explode зменшує дані
3. **Join Reordering**: Catalyst optimizer оптимізує порядок JOIN
4. **Post-Aggregation Filter**: >= 3 фільтрує режисерів з малою кількістю фільмів

**Особливості:**
- **4-Way Join**: Складна операція, потребує ретельного планування
- **Explode Cost**: Збільшує dataset, але необхідно для багатозначних полів
- **Shuffle для GROUP BY**: Необхідний для агрегації

**Очікувана продуктивність:** Низька (30-50 сек через 4 JOIN + explode + shuffle)

---

### Питання 2.2: Актори, які знімалися в найбільшій кількості жанрів

**Бізнес-ціль:** Знайти універсальних акторів для різножанрових проєктів.

**Застосовані операції:**
- `FILTER`: category IN (actor, actress), titleType, isAdult, movies_count >= 10
- `JOIN`: title.principals + title.basics + name.basics
- `GROUP BY`: nconst, primaryName
- `EXPLODE`: жанри
- `countDistinct`: лічба унікальних жанрів

**PySpark код:**
```python
q2_2 = df_principals \
    .filter(col("category").isin(["actor", "actress"])) \
    .join(df_basics, "tconst") \
    .filter((col("titleType") == "movie") & (col("isAdult") == 0)) \
    .join(df_names, "nconst") \
    .withColumn("genre", explode(split(col("genres"), ","))) \
    .groupBy("nconst", "primaryName") \
    .agg(
        countDistinct("genre").alias("genres_count"),
        count("tconst").alias("movies_count"),
        collect_set("genre").alias("genres_list")
    ) \
    .filter(col("movies_count") >= 10) \
    .orderBy(col("genres_count").desc()) \
    .limit(15)
```

**Аналіз плану виконання:**

```
Physical Plan:
├── TakeOrderedAndProject [limit=15, orderBy=genres_count DESC]
│   └── Filter [movies_count >= 10]
│       └── HashAggregate [final: countDistinct, count, collect_set]
│           └── Exchange [HashPartitioning(nconst, primaryName)]
│               └── HashAggregate [partial aggregates]
│                   └── Generate [explode(genres)]
│                       └── BroadcastHashJoin [nconst]
│                           ├── BroadcastHashJoin [tconst]
│                           │   ├── Filter [principals: category]
│                           │   └── Filter [basics: titleType, isAdult]
│                           └── BroadcastExchange [names]
```

**Оптимізації:**
1. **countDistinct Optimization**: Використовує HyperLogLog для approximate count (якщо enabled)
2. **collect_set**: Зберігає унікальні значення (може бути memory-intensive)
3. **Partial Aggregation**: countDistinct може частково агрегуватися локально

**Потенційні проблеми:**
- **Explode Inflation**: Кожен фільм з 3 жанрами → 3 рядки
- **collect_set Memory**: Зберігає всі унікальні жанри для кожного актора
- **Large Shuffle**: principals table велика (~50M рядків)

**Очікувана продуктивність:** Дуже низька (60-90 сек через велику principals table)

---

### Питання 2.3: Живі актори старше 70 років, які активні після 2010

**Бізнес-ціль:** Знайти досвідчених акторів, які продовжують кар'єру.

**Застосовані операції:**
- `FILTER`: deathYear IS NULL, birthYear IS NOT NULL, birthYear <= 1956, profession contains actor/actress, startYear >= 2010, titleType
- `JOIN`: name.basics + title.principals + title.basics

**PySpark код:**
```python
q2_3 = df_names \
    .filter(
        (col("deathYear").isNull()) & 
        (col("birthYear").isNotNull()) & 
        (col("birthYear") <= 1956) &
        (col("primaryProfession").contains("actor") | col("primaryProfession").contains("actress"))
    ) \
    .join(df_principals, "nconst") \
    .join(df_basics, "tconst") \
    .filter((col("startYear") >= 2010) & (col("titleType") == "movie")) \
    .withColumn("age", 2026 - col("birthYear")) \
    .groupBy("nconst", "primaryName", "birthYear", "age") \
    .agg(count("*").alias("recent_movies")) \
    .orderBy(col("recent_movies").desc(), col("age").desc()) \
    .limit(20)
```

**Аналіз плану виконання:**

```
Physical Plan:
├── TakeOrderedAndProject [limit=20]
│   └── HashAggregate [final]
│       └── Exchange [HashPartitioning(nconst, primaryName, birthYear, age)]
│           └── HashAggregate [partial]
│               └── Project [calculate age]
│                   └── BroadcastHashJoin [tconst]
│                       ├── Filter [startYear >= 2010, titleType]
│                       └── BroadcastHashJoin [nconst]
│                           ├── Filter [principals table]
│                           └── BroadcastExchange
│                               └── Filter [names: complex conditions]
```

**Оптимізації:**
1. **Highly Selective Filter**: birthYear <= 1956 AND deathYear IS NULL сильно зменшує names
2. **String Contains**: contains() може бути costly, але застосовується на малому dataset
3. **Year Range Filter**: startYear >= 2010 фільтрує ~90% фільмів

**Очікувана продуктивність:** Середня (10-20 сек, selective filters допомагають)

---

### Питання 2.4: Ранжування акторів по середньому рейтингу фільмів у кожному десятилітті

**Бізнес-ціль:** Визначити зірок різних епох для ретроспективного аналізу.

**Застосовані операції:**
- `FILTER`: category, titleType, startYear IS NOT NULL, numVotes >= 1000, movies_count >= 3, rank <= 5
- `JOIN`: title.principals + title.basics + title.ratings + name.basics
- `GROUP BY`: nconst, primaryName, decade
- `WINDOW FUNCTION`: row_number() OVER (PARTITION BY decade)

**PySpark код:**
```python
q2_4 = df_principals \
    .filter(col("category").isin(["actor", "actress"])) \
    .join(df_basics, "tconst") \
    .filter((col("titleType") == "movie") & col("startYear").isNotNull()) \
    .join(df_ratings, "tconst") \
    .filter(col("numVotes") >= 1000) \
    .join(df_names, "nconst") \
    .withColumn("decade", (floor(col("startYear") / 10) * 10).cast("int")) \
    .groupBy("nconst", "primaryName", "decade") \
    .agg(
        round(avg("averageRating"), 2).alias("avg_rating"),
        count("*").alias("movies_count")
    ) \
    .filter(col("movies_count") >= 3) \
    .withColumn(
        "rank_in_decade",
        row_number().over(Window.partitionBy("decade").orderBy(col("avg_rating").desc()))
    ) \
    .filter(col("rank_in_decade") <= 5)
```

**Аналіз плану виконання:**

```
Physical Plan:
├── Filter [rank_in_decade <= 5]
│   └── Window [row_number() OVER (PARTITION BY decade ORDER BY avg_rating DESC)]
│       └── Sort [decade, avg_rating DESC]
│           └── Exchange [HashPartitioning(decade)]
│               └── Filter [movies_count >= 3]
│                   └── HashAggregate [final: (nconst, primaryName, decade)]
│                       └── Exchange [HashPartitioning(nconst, primaryName, decade)]
│                           └── HashAggregate [partial]
│                               └── 4-way JOIN chain
```

**Оптимізації:**
1. **Two-Level Aggregation**: Спочатку GROUP BY (actor, decade), потім WINDOW
2. **Filter Between Stages**: movies_count >= 3 фільтрує перед window
3. **Partitioned Window**: decade має ~15 значень, зменшує partition size

**Особливості:**
- **Double Shuffle**: Один для GROUP BY, один для WINDOW
- **Complex Pipeline**: 4 JOIN + GROUP BY + WINDOW
- **Memory Usage**: Зберігання агрегованих даних для window

**Очікувана продуктивність:** Дуже низька (60-120 сек через складність)

---

### Питання 2.5: Співпраці режисер-актор з найвищими рейтингами

**Бізнес-ціль:** Знайти успішні творчі дуети для майбутніх проєктів.

**Застосовані операції:**
- `FILTER`: directors IS NOT NULL, category, titleType, isAdult, averageRating >= 7.0, collaborations >= 3, rank <= 10
- `JOIN`: title.crew + title.principals + title.basics + title.ratings + name.basics (x2) = 6 JOIN!
- `GROUP BY`: (director_id, director_name, actor_id, actor_name)
- `WINDOW FUNCTION`: row_number() для фінального ранжування
- `EXPLODE`: directors

**PySpark код:**
```python
q2_5 = df_crew \
    .filter(col("directors").isNotNull()) \
    .withColumn("director_id", explode(split(col("directors"), ","))) \
    .join(df_principals.filter(col("category").isin(["actor", "actress"])), "tconst") \
    .join(df_basics, "tconst") \
    .filter((col("titleType") == "movie") & (col("isAdult") == 0)) \
    .join(df_ratings, "tconst") \
    .filter(col("averageRating") >= 7.0) \
    .join(df_names.alias("director"), col("director_id") == col("director.nconst")) \
    .join(df_names.alias("actor"), df_principals.nconst == col("actor.nconst")) \
    .groupBy(...) \
    .agg(count, avg) \
    .filter(col("collaborations") >= 3) \
    .withColumn("rank", row_number().over(Window.orderBy(...))) \
    .filter(col("rank") <= 10)
```

**Аналіз плану виконання (extended):**

```
Physical Plan:
├── Filter [rank <= 10]
│   └── Window [row_number() OVER (ORDER BY avg_rating DESC, collaborations DESC)]
│       └── Sort [global sort]
│           └── Exchange [SinglePartition] ← EXPENSIVE!
│               └── Filter [collaborations >= 3]
│                   └── HashAggregate [final]
│                       └── Exchange [HashPartitioning(director_id, director_name, actor_id, actor_name)]
│                           └── HashAggregate [partial]
│                               └── BroadcastHashJoin [actor.nconst] (join #6)
│                                   ├── BroadcastExchange [names table - actor alias]
│                                   └── BroadcastHashJoin [director.nconst] (join #5)
│                                       ├── BroadcastExchange [names table - director alias]
│                                       └── BroadcastHashJoin [tconst] (join #4)
│                                           ├── Filter [averageRating >= 7.0]
│                                           └── BroadcastHashJoin [tconst] (join #3)
│                                               ├── Filter [basics]
│                                               └── BroadcastHashJoin [tconst] (join #2)
│                                                   ├── Filter [principals - category]
│                                                   └── Generate [explode(directors)] (join #1)
```

**Оптимізації:**
1. **Multiple Broadcast Joins**: Більшість таблиць broadcast-яться
2. **Self-Join Optimization**: names table приєднується двічі з різними alias
3. **Filter Pushdown**: Всі фільтри застосовуються якомога раніше

**Проблеми:**
1. **SinglePartition Exchange**: Window без PARTITION BY потребує збору всіх даних на одну ноду!
2. **6-Way Join**: Надзвичайно складна операція
3. **Explode × Principals**: Комбінаторний вибух даних
4. **Memory Pressure**: GROUP BY по 4 колонках створює багато груп

**Рекомендації по оптимізації:**
- Розглянути approx_count_distinct замість exact count
- Обмежити decade для зменшення dataset
- Використати JOIN HINT для контролю broadcast thresholds

**Очікувана продуктивність:** Критично низька (2-5 хвилин!)

---

### Питання 2.6: Порівняння продуктивності акторів у різних жанрах

**Бізнес-ціль:** Визначити спеціалізацію акторів для кастингу.

**Застосовані операції:**
- `FILTER`: category, titleType, isAdult, numVotes >= 500, movies_in_genre >= 5
- `JOIN`: title.principals + title.basics + title.ratings + name.basics
- `GROUP BY`: (nconst, primaryName, genre)
- `EXPLODE`: genres

**PySpark код:**
```python
q2_6 = df_principals \
    .filter(col("category").isin(["actor", "actress"])) \
    .join(df_basics, "tconst") \
    .filter((col("titleType") == "movie") & (col("isAdult") == 0)) \
    .join(df_ratings, "tconst") \
    .filter(col("numVotes") >= 500) \
    .join(df_names, "nconst") \
    .withColumn("genre", explode(split(col("genres"), ","))) \
    .groupBy("nconst", "primaryName", "genre") \
    .agg(
        count("*").alias("movies_in_genre"),
        round(avg("averageRating"), 2).alias("avg_rating_in_genre")
    ) \
    .filter(col("movies_in_genre") >= 5) \
    .orderBy(col("avg_rating_in_genre").desc()) \
    .limit(20)
```

**Аналіз плану виконання:**

```
Physical Plan:
├── TakeOrderedAndProject [limit=20]
│   └── Filter [movies_in_genre >= 5]
│       └── HashAggregate [final: (nconst, primaryName, genre)]
│           └── Exchange [HashPartitioning(nconst, primaryName, genre)]
│               └── HashAggregate [partial]
│                   └── Generate [explode(genres)]
│                       └── 4-way JOIN chain
```

**Оптимізації:**
1. **Three-Column GROUP BY**: Висока кардинальність, але необхідна для аналізу
2. **Post-Aggregation Filter**: >= 5 фільтрує рідкісні комбінації
3. **Explode After JOIN**: Зменшує розмір до explode

**Очікувана продуктивність:** Низька (40-60 сек через велику principals + explode)

---

## ОСОБА 3: Аналіз серіалів та епізодів

### Питання 3.1: ТОП-10 серіалів з найвищим середнім рейтингом епізодів

**Бізнес-ціль:** Визначити найякісніші серіали за середньою оцінкою всіх епізодів.

**Застосовані операції:**
- `FILTER`: titleType == tvSeries, episodes_count >= 10
- `JOIN`: title.episode + title.basics (episode) + title.ratings (episode) + title.basics (series)
- `GROUP BY`: parentTconst, series_title

**PySpark код:**
```python
q3_1 = df_episodes \
    .join(df_basics.alias("episode"), df_episodes.tconst == col("episode.tconst")) \
    .join(df_ratings.alias("episode_rating"), df_episodes.tconst == col("episode_rating.tconst")) \
    .join(df_basics.alias("series"), df_episodes.parentTconst == col("series.tconst")) \
    .filter(col("series.titleType") == "tvSeries") \
    .groupBy("parentTconst", col("series.primaryTitle").alias("series_title")) \
    .agg(
        count("*").alias("episodes_count"),
        round(avg("episode_rating.averageRating"), 2).alias("avg_episode_rating"),
        sum("episode_rating.numVotes").alias("total_votes")
    ) \
    .filter(col("episodes_count") >= 10) \
    .orderBy(col("avg_episode_rating").desc()) \
    .limit(10)
```

**Аналіз плану виконання:**

```
Physical Plan:
├── TakeOrderedAndProject [limit=10]
│   └── Filter [episodes_count >= 10]
│       └── HashAggregate [final: groupBy=(parentTconst, series_title)]
│           └── Exchange [HashPartitioning(parentTconst, series_title)]
│               └── HashAggregate [partial]
│                   └── BroadcastHashJoin [parentTconst = series.tconst]
│                       ├── BroadcastExchange [basics table - series]
│                       └── BroadcastHashJoin [tconst = episode_rating.tconst]
│                           ├── BroadcastExchange [ratings table]
│                           └── BroadcastHashJoin [tconst = episode.tconst]
│                               ├── FileScan [episodes]
│                               └── BroadcastExchange [basics - episode]
```

**Оптимізації:**
1. **Three Broadcast Joins**: Всі lookup таблиці broadcast-яться
2. **Self-Join on basics**: Таблиця basics використовується двічі (episode + series)
3. **Filter After Aggregation**: >= 10 episodes зменшує фінальний результат

**Особливості:**
- **Episode-Level Join**: Кожен епізод приєднується до 3 таблиць
- ~8M епізодів у датасеті
- GROUP BY по parentTconst агрегує епізоди в серіали

**Очікувана продуктивність:** Середня (20-30 сек через 8M епізодів)

---

### Питання 3.2: Серіали з найбільшою різницею між найкращими та найгіршими епізодами

**Бізнес-ціль:** Знайти серіали з непостійною якістю для аналізу причин.

**Застосовані операції:**
- `FILTER`: titleType == tvSeries, episodes_count >= 20
- `JOIN`: title.episode + title.ratings + title.basics (series) + title.basics (episode)
- `WINDOW FUNCTION`: min(), max() OVER (PARTITION BY parentTconst)
- `GROUP BY`: parentTconst, series_title

**PySpark код:**
```python
q3_2 = df_episodes \
    .join(df_ratings.alias("episode_rating"), df_episodes.tconst == col("episode_rating.tconst")) \
    .join(df_basics.alias("series"), df_episodes.parentTconst == col("series.tconst")) \
    .join(df_basics.alias("episode"), df_episodes.tconst == col("episode.tconst")) \
    .filter(col("series.titleType") == "tvSeries") \
    .withColumn("max_rating", max("episode_rating.averageRating").over(Window.partitionBy("parentTconst"))) \
    .withColumn("min_rating", min("episode_rating.averageRating").over(Window.partitionBy("parentTconst"))) \
    .withColumn("rating_range", col("max_rating") - col("min_rating")) \
    .groupBy("parentTconst", col("series.primaryTitle").alias("series_title")) \
    .agg(
        first("rating_range").alias("rating_range"),
        first("max_rating").alias("best_episode_rating"),
        first("min_rating").alias("worst_episode_rating"),
        count("*").alias("episodes_count")
    ) \
    .filter(col("episodes_count") >= 20) \
    .orderBy(col("rating_range").desc()) \
    .limit(15)
```

**Аналіз плану виконання:**

```
Physical Plan:
├── TakeOrderedAndProject [limit=15]
│   └── Filter [episodes_count >= 20]
│       └── HashAggregate [final: first(rating_range), first(max_rating), etc.]
│           └── Exchange [HashPartitioning(parentTconst, series_title)]
│               └── HashAggregate [partial: first aggregates, count]
│                   └── Project [calculate rating_range]
│                       └── Window [min/max OVER (PARTITION BY parentTconst)]
│                           └── Sort [parentTconst]
│                               └── Exchange [HashPartitioning(parentTconst)]
│                                   └── 4-way JOIN chain
```

**Оптимізації:**
1. **Aggregating Window Functions**: min() та max() ефективні для window
2. **Single Window Pass**: Обидві функції обчислюються за один прохід
3. **first() Aggregate**: Ефективно бере перше значення (всі однакові в групі)

**Особливості:**
- **Window + GroupBy**: Подвійна агрегація (window по епізодах, потім groupBy по серіалах)
- **Double Shuffle**: Один для window, один для groupBy
- **Redundant Calculation**: rating_range обчислюється для кожного рядка, але однакове у серіалі

**Оптимізація:** Можна спростити, використавши тільки GROUP BY з min/max агрегатами без window:
```python
.groupBy("parentTconst")
.agg(
    max("rating").alias("max_rating"),
    min("rating").alias("min_rating")
)
.withColumn("rating_range", col("max_rating") - col("min_rating"))
```

**Очікувана продуктивність:** Низька (40-60 сек через window + groupBy + 4 JOIN)

---

### Питання 3.3: Епізоди серіалів доступні українською

**Бізнес-ціль:** Знайти якісний серіальний контент для українського ринку.

**Застосовані операції:**
- `FILTER`: region == UA OR language == uk, titleType == tvSeries, averageRating >= 8.0
- `JOIN`: title.akas + title.episode + title.basics (series) + title.ratings

**PySpark код:**
```python
q3_3 = df_akas \
    .filter((col("region") == "UA") | (col("language") == "uk")) \
    .join(df_episodes, df_akas.titleId == df_episodes.tconst) \
    .join(df_basics.alias("series"), df_episodes.parentTconst == col("series.tconst")) \
    .filter(col("series.titleType") == "tvSeries") \
    .join(df_ratings, df_episodes.tconst == df_ratings.tconst) \
    .filter(col("averageRating") >= 8.0) \
    .select(...) \
    .orderBy(col("averageRating").desc()) \
    .limit(20)
```

**Аналіз плану виконання:**

```
Physical Plan:
├── TakeOrderedAndProject [limit=20]
│   └── Project [selected columns]
│       └── BroadcastHashJoin [tconst]
│           ├── Filter [averageRating >= 8.0]
│           └── Filter [titleType = tvSeries]
│               └── BroadcastHashJoin [parentTconst]
│                   ├── BroadcastExchange [basics - series]
│                   └── BroadcastHashJoin [titleId = tconst]
│                       ├── FileScan [episodes]
│                       └── BroadcastExchange
│                           └── Filter [region=UA OR language=uk]
│                               └── FileScan [akas]
```

**Оптимізації:**
1. **Highly Selective First Filter**: UA filter сильно зменшує akas table
2. **Broadcast Smaller Tables**: Filtered akas, basics, ratings broadcast-яться
3. **Filter Pushdown**: Всі фільтри застосовуються перед JOIN

**Очікувана продуктивність:** Висока (5-10 сек завдяки selective UA filter)

---

### Питання 3.4: Динаміка рейтингів по сезонах для кожного серіалу

**Бізнес-ціль:** Аналіз зміни якості серіалів протягом сезонів для прогнозування.

**Застосовані операції:**
- `FILTER`: seasonNumber IS NOT NULL, titleType == tvSeries, seasonNumber <= 10
- `JOIN`: title.episode + title.ratings + title.basics
- `GROUP BY`: (parentTconst, primaryTitle, seasonNumber)
- `WINDOW FUNCTION`: lag() OVER (PARTITION BY parentTconst ORDER BY seasonNumber)

**PySpark код:**
```python
q3_4 = df_episodes \
    .filter(col("seasonNumber").isNotNull()) \
    .join(df_ratings, df_episodes.tconst == df_ratings.tconst) \
    .join(df_basics, df_episodes.parentTconst == df_basics.tconst) \
    .filter(col("titleType") == "tvSeries") \
    .groupBy("parentTconst", "primaryTitle", "seasonNumber") \
    .agg(
        round(avg("averageRating"), 2).alias("season_avg_rating"),
        count("*").alias("episodes_in_season")
    ) \
    .withColumn(
        "prev_season_rating",
        lag("season_avg_rating").over(Window.partitionBy("parentTconst").orderBy("seasonNumber"))
    ) \
    .withColumn(
        "rating_change",
        when(col("prev_season_rating").isNotNull(), 
             round(col("season_avg_rating") - col("prev_season_rating"), 2))
        .otherwise(lit(None))
    ) \
    .filter(col("seasonNumber") <= 10) \
    .select(...)
```

**Аналіз плану виконання:**

```
Physical Plan:
├── Filter [seasonNumber <= 10]
│   └── Project [calculate rating_change]
│       └── Window [lag(season_avg_rating) OVER (PARTITION BY parentTconst ORDER BY seasonNumber)]
│           └── Sort [parentTconst, seasonNumber]
│               └── Exchange [HashPartitioning(parentTconst)]
│                   └── HashAggregate [final: (parentTconst, title, seasonNumber)]
│                       └── Exchange [HashPartitioning(parentTconst, primaryTitle, seasonNumber)]
│                           └── HashAggregate [partial]
│                               └── 3-way JOIN
```

**Оптимізації:**
1. **lag() Window Function**: Ефективна для часових серій
2. **Ordered Window**: ORDER BY seasonNumber для правильної послідовності
3. **Two-Phase Aggregation**: Partial + Final для GROUP BY
4. **Post-Window Filter**: seasonNumber <= 10 застосовується після window

**Особливості:**
- **GroupBy + Window**: Спочатку агрегація по сезонах, потім lag по серіалах
- **Nullable lag**: Перший сезон має NULL в prev_season_rating
- **Partitioned Sort**: Сортування в межах кожного серіалу

**Очікувана продуктивність:** Середня (25-35 сек через window на агрегованих даних)

---

### Питання 3.5: Серіали з найкращими фінальними сезонами

**Бізнес-ціль:** Визначити серіали з якісним завершенням для рекомендацій.

**Застосовані операції:**
- `FILTER`: seasonNumber IS NOT NULL, titleType == tvSeries, max_season >= 3, final_season_episodes >= 5
- `JOIN`: title.episode + title.ratings + title.basics
- `WINDOW FUNCTION`: max(seasonNumber) OVER (PARTITION BY parentTconst)
- `GROUP BY`: (parentTconst, primaryTitle, max_season)

**PySpark код:**
```python
q3_5 = df_episodes \
    .filter(col("seasonNumber").isNotNull()) \
    .join(df_ratings, df_episodes.tconst == df_ratings.tconst) \
    .join(df_basics, df_episodes.parentTconst == df_basics.tconst) \
    .filter(col("titleType") == "tvSeries") \
    .withColumn(
        "max_season",
        max("seasonNumber").over(Window.partitionBy("parentTconst"))
    ) \
    .filter(col("seasonNumber") == col("max_season")) \
    .groupBy("parentTconst", "primaryTitle", "max_season") \
    .agg(
        round(avg("averageRating"), 2).alias("final_season_rating"),
        count("*").alias("final_season_episodes")
    ) \
    .filter((col("max_season") >= 3) & (col("final_season_episodes") >= 5)) \
    .orderBy(col("final_season_rating").desc()) \
    .limit(15)
```

**Аналіз плану виконання:**

```
Physical Plan:
├── TakeOrderedAndProject [limit=15]
│   └── Filter [max_season >= 3 AND final_season_episodes >= 5]
│       └── HashAggregate [final: (parentTconst, title, max_season)]
│           └── Exchange [HashPartitioning(...)]
│               └── HashAggregate [partial]
│                   └── Filter [seasonNumber = max_season]
│                       └── Window [max(seasonNumber) OVER (PARTITION BY parentTconst)]
│                           └── Sort [parentTconst]
│                               └── Exchange [HashPartitioning(parentTconst)]
│                                   └── 3-way JOIN
```

**Оптимізації:**
1. **Window для визначення max_season**: Одне обчислення на серіал
2. **Filter на Window Result**: Залишає тільки епізоди фінального сезону
3. **Post-Aggregation Filter**: Фільтрує серіали з малою кількістю сезонів

**Особливості:**
- **Window + Filter + GroupBy**: Три стадії обробки
- **Selective Filter**: seasonNumber == max_season сильно зменшує дані перед GROUP BY
- Середній серіал має ~5 сезонів, тому filter залишає ~20% епізодів

**Очікувана продуктивність:** Середня (20-30 сек)

---

### Питання 3.6: Порівняння популярності різних типів серіалів

**Бізнес-ціль:** Аналіз трендів типів серіального контенту по декадах.

**Застосовані операції:**
- `FILTER`: titleType IN (tvSeries, tvMiniSeries, tvMovie), isAdult == 0, startYear IS NOT NULL, decade >= 1990
- `JOIN`: title.basics + title.ratings
- `GROUP BY`: (decade, titleType)

**PySpark код:**
```python
q3_6 = df_basics \
    .filter(col("titleType").isin(["tvSeries", "tvMiniSeries", "tvMovie"])) \
    .filter((col("isAdult") == 0) & col("startYear").isNotNull()) \
    .join(df_ratings, "tconst") \
    .withColumn("decade", (floor(col("startYear") / 10) * 10).cast("int")) \
    .filter(col("decade") >= 1990) \
    .groupBy("decade", "titleType") \
    .agg(
        count("*").alias("count"),
        round(avg("averageRating"), 2).alias("avg_rating"),
        round(avg("numVotes"), 0).alias("avg_popularity")
    ) \
    .orderBy("decade", col("avg_rating").desc())
```

**Аналіз плану виконання:**

```
Physical Plan:
├── Sort [decade ASC, avg_rating DESC]
│   └── HashAggregate [final: (decade, titleType)]
│       └── Exchange [HashPartitioning(decade, titleType)]
│           └── HashAggregate [partial]
│               └── Project [calculate decade]
│                   └── Filter [decade >= 1990]
│                       └── BroadcastHashJoin [tconst]
│                           ├── Filter [titleType IN (...), isAdult, startYear]
│                           └── BroadcastExchange [ratings]
```

**Оптимізації:**
1. **IN Predicate**: Ефективна фільтрація по titleType
2. **Low Cardinality GROUP BY**: decade (4 значення) × titleType (3 значення) = 12 груп
3. **Decade Calculation**: Виконується після JOIN (може бути pushed down)

**Очікувана продуктивність:** Висока (5-10 сек через малу кількість груп)

---

## ОСОБА 4: Географічний та мовний аналіз

### Питання 4.1: Кількість локалізацій для найпопулярніших фільмів

**Бізнес-ціль:** Визначити найбільш міжнародні фільми для глобального маркетингу.

**Застосовані операції:**
- `FILTER`: titleType, isAdult, numVotes >= 50000
- `JOIN`: title.basics + title.ratings + title.akas
- `GROUP BY`: (tconst, primaryTitle, averageRating, numVotes)
- `countDistinct`: регіони та мови

**PySpark код:**
```python
q4_1 = df_basics \
    .filter((col("titleType") == "movie") & (col("isAdult") == 0)) \
    .join(df_ratings, "tconst") \
    .filter(col("numVotes") >= 50000) \
    .join(df_akas, df_basics.tconst == df_akas.titleId) \
    .groupBy("tconst", "primaryTitle", "averageRating", "numVotes") \
    .agg(
        countDistinct("region").alias("regions_count"),
        countDistinct("language").alias("languages_count"),
        count("*").alias("total_versions")
    ) \
    .orderBy(col("regions_count").desc(), col("languages_count").desc()) \
    .limit(20)
```

**Аналіз плану виконання:**

```
Physical Plan:
├── TakeOrderedAndProject [limit=20]
│   └── HashAggregate [final: countDistinct(region), countDistinct(language), count]
│       └── Exchange [HashPartitioning(tconst, title, rating, votes)]
│           └── HashAggregate [partial: countDistinct]
│               └── BroadcastHashJoin [tconst = titleId]
│                   ├── FileScan [akas] ← Large table!
│                   └── BroadcastExchange
│                       └── Filter [numVotes >= 50000]
│                           └── BroadcastHashJoin [tconst]
│                               ├── Filter [basics]
│                               └── Filter [ratings]
```

**Оптимізації:**
1. **Selective Filter First**: numVotes >= 50000 зменшує dataset до ~5000 фільмів
2. **countDistinct Partial Aggregation**: Часткова агрегація локально
3. **Broadcast Filtered Tables**: Малий набір фільмів broadcast-иться

**Особливості:**
- **akas table велика**: ~36M рядків, але JOIN на 5000 фільмів ефективний
- **Multiple countDistinct**: Потребує зберігання унікальних значень у пам'яті
- **High Cardinality**: regions ~200, languages ~400

**Очікувана продуктивність:** Середня (15-25 сек через велику akas table)

---

### Питання 4.2: Найпопулярніші жанри в різних країнах

**Бізнес-ціль:** Визначити регіональні переваги жанрів для таргетованого маркетингу.

**Застосовані операції:**
- `FILTER`: region IN (...), titleType, isAdult, numVotes >= 100, rank_in_region <= 3
- `JOIN`: title.akas + title.basics + title.ratings
- `GROUP BY`: (region, genre)
- `WINDOW FUNCTION`: row_number() OVER (PARTITION BY region ORDER BY movies_count DESC)
- `EXPLODE`: genres

**PySpark код:**
```python
q4_2 = df_akas \
    .filter(col("region").isNotNull() & col("region").isin("US", "GB", "FR", "DE", "JP", "KR", "IN", "BR", "UA")) \
    .join(df_basics, df_akas.titleId == df_basics.tconst) \
    .filter((col("titleType") == "movie") & (col("isAdult") == 0)) \
    .join(df_ratings, "tconst") \
    .filter(col("numVotes") >= 100) \
    .withColumn("genre", explode(split(col("genres"), ","))) \
    .groupBy("region", "genre") \
    .agg(
        count("*").alias("movies_count"),
        round(avg("averageRating"), 2).alias("avg_rating")
    ) \
    .withColumn(
        "rank_in_region",
        row_number().over(Window.partitionBy("region").orderBy(col("movies_count").desc()))
    ) \
    .filter(col("rank_in_region") <= 3) \
    .select(...)
```

**Аналіз плану виконання:**

```
Physical Plan:
├── Project [final selection]
│   └── Filter [rank_in_region <= 3]
│       └── Window [row_number() OVER (PARTITION BY region ORDER BY movies_count DESC)]
│           └── Sort [region, movies_count DESC]
│               └── Exchange [HashPartitioning(region)]
│                   └── HashAggregate [final: (region, genre)]
│                       └── Exchange [HashPartitioning(region, genre)]
│                           └── HashAggregate [partial]
│                               └── Generate [explode(genres)]
│                                   └── 3-way JOIN
```

**Оптимізації:**
1. **IN Filter**: 9 країн сильно зменшує akas
2. **Window Partitioning**: Тільки 9 партицій (по країнах)
3. **row_number()**: Ефективна window function
4. **Post-Window Filter**: Залишає тільки ТОП-3 per region (27 рядків)

**Особливості:**
- **Explode після JOIN**: Збільшує дані після фільтрації
- **Two-Level Grouping**: GROUP BY + WINDOW
- **Double Shuffle**: Для GROUP BY та WINDOW

**Очікувана продуктивність:** Середня (20-30 сек)

---

### Питання 4.3: Фільми з найбільшою кількістю альтернативних назв

**Бізнес-ціль:** Знайти фільми з найширшою міжнародною дистрибуцією.

**Застосовані операції:**
- `FILTER`: titleType, isAdult, title != primaryTitle, alternative_titles_count >= 10
- `JOIN`: title.akas + title.basics
- `GROUP BY`: (titleId, primaryTitle)
- `collect_set`: зібрати приклади назв

**PySpark код:**
```python
q4_3 = df_akas \
    .join(df_basics, df_akas.titleId == df_basics.tconst) \
    .filter((col("titleType") == "movie") & (col("isAdult") == 0)) \
    .filter(col("title") != col("primaryTitle")) \
    .groupBy("titleId", "primaryTitle") \
    .agg(
        count("*").alias("alternative_titles_count"),
        countDistinct("region").alias("unique_regions"),
        collect_set("title").alias("some_alternative_titles")
    ) \
    .filter(col("alternative_titles_count") >= 10) \
    .orderBy(col("alternative_titles_count").desc()) \
    .limit(15)
```

**Аналіз плану виконання:**

```
Physical Plan:
├── TakeOrderedAndProject [limit=15]
│   └── Filter [alternative_titles_count >= 10]
│       └── HashAggregate [final: count, countDistinct, collect_set]
│           └── Exchange [HashPartitioning(titleId, primaryTitle)]
│               └── HashAggregate [partial aggregates]
│                   └── Filter [title != primaryTitle]
│                       └── BroadcastHashJoin [titleId = tconst]
│                           ├── FileScan [akas] ← 36M rows
│                           └── BroadcastExchange
│                               └── Filter [titleType, isAdult]
│                                   └── FileScan [basics]
```

**Оптимізації:**
1. **Filter After JOIN**: title != primaryTitle вимагає обидва поля
2. **collect_set**: Зберігає тільки унікальні назви (deduplicate automatically)
3. **Post-Aggregation Filter**: >= 10 фільтрує більшість фільмів

**Особливості:**
- **collect_set Memory**: Може зберігати сотні назв для популярних фільмів
- **Large akas Scan**: 36M рядків обробляються
- **String Comparison**: title != primaryTitle costly на великих даних

**Очікувана продуктивність:** Низька (40-60 сек через велику akas table)

---

### Питання 4.4: Середній рейтинг фільмів по мовах

**Бізнес-ціль:** Визначити мови з найякіснішим кіноконтентом.

**Застосовані операції:**
- `FILTER`: language IS NOT NULL, titleType, isAdult, numVotes >= 500, movies_count >= 50
- `JOIN`: title.akas + title.basics + title.ratings
- `GROUP BY`: language
- `WINDOW FUNCTION`: row_number() для ранжування

**PySpark код:**
```python
q4_4 = df_akas \
    .filter(col("language").isNotNull()) \
    .join(df_basics, df_akas.titleId == df_basics.tconst) \
    .filter((col("titleType") == "movie") & (col("isAdult") == 0)) \
    .join(df_ratings, "tconst") \
    .filter(col("numVotes") >= 500) \
    .groupBy("language") \
    .agg(
        count(col("tconst")).alias("movies_count"),
        round(avg("averageRating"), 2).alias("avg_rating"),
        sum("numVotes").alias("total_votes")
    ) \
    .filter(col("movies_count") >= 50) \
    .withColumn(
        "rating_rank",
        row_number().over(Window.orderBy(col("avg_rating").desc()))
    ) \
    .select(...) \
    .orderBy("rating_rank") \
    .limit(20)
```

**Аналіз плану виконання:**

```
Physical Plan:
├── TakeOrderedAndProject [limit=20]
│   └── Project
│       └── Window [row_number() OVER (ORDER BY avg_rating DESC)]
│           └── Sort [avg_rating DESC]
│               └── Exchange [SinglePartition] ← Collect all data!
│                   └── Filter [movies_count >= 50]
│                       └── HashAggregate [final: (language)]
│                           └── Exchange [HashPartitioning(language)]
│                               └── HashAggregate [partial]
│                                   └── 3-way JOIN
```

**Оптимізації:**
1. **Language Filter**: IS NOT NULL зменшує ~50% akas
2. **Low Cardinality GROUP BY**: ~400 мов
3. **Post-Aggregation Filter**: >= 50 фільтрує рідкісні мови

**Проблема:**
- **SinglePartition Exchange**: Window без PARTITION BY збирає ВСІ дані на one node!
- Після фільтрації залишається ~100-150 мов, тому cost не критичний

**Очікувана продуктивність:** Середня (20-30 сек)

---

### Питання 4.5: Порівняння локалізації фільмів по десятиліттях

**Бізнес-ціль:** Аналіз тренду глобалізації кіноіндустрії.

**Застосовані операції:**
- `FILTER`: titleType, isAdult, startYear IS NOT NULL, decade >= 1950
- `JOIN`: title.basics + title.akas
- `GROUP BY`: (tconst, primaryTitle, decade) → потім по decade
- `countDistinct`: regions per film

**PySpark код:**
```python
q4_5 = df_basics \
    .filter((col("titleType") == "movie") & (col("isAdult") == 0) & col("startYear").isNotNull()) \
    .withColumn("decade", (floor(col("startYear") / 10) * 10).cast("int")) \
    .filter(col("decade") >= 1950) \
    .join(df_akas, df_basics.tconst == df_akas.titleId) \
    .groupBy("tconst", "primaryTitle", "decade") \
    .agg(countDistinct("region").alias("localization_count")) \
    .groupBy("decade") \
    .agg(
        count("*").alias("films_count"),
        round(avg("localization_count"), 1).alias("avg_localizations"),
        max("localization_count").alias("max_localizations")
    ) \
    .orderBy("decade")
```

**Аналіз плану виконання:**

```
Physical Plan:
├── Sort [decade ASC]
│   └── HashAggregate [final: groupBy=decade, agg={count, avg, max}]
│       └── Exchange [HashPartitioning(decade)]
│           └── HashAggregate [partial]
│               └── HashAggregate [final: groupBy=(tconst, title, decade), countDistinct(region)]
│                   └── Exchange [HashPartitioning(tconst, primaryTitle, decade)]
│                       └── HashAggregate [partial: countDistinct]
│                           └── BroadcastHashJoin [tconst = titleId]
│                               ├── FileScan [akas]
│                               └── BroadcastExchange
│                                   └── Filter & Project [decade calculation]
```

**Оптимізації:**
1. **Two-Level GROUP BY**: Nested aggregation pattern
2. **countDistinct Partial Aggregation**: Localized counting
3. **Decade Pre-calculation**: Computed before JOIN

**Особливості:**
- **Double Shuffle**: По (tconst, title, decade), потім по decade
- **Large Intermediate**: Region count per film зберігається у пам'яті
- **Decade має low cardinality**: ~8 значень (1950-2020)

**Очікувана продуктивність:** Середня (25-35 сек через nested GROUP BY)

---

### Питання 4.6: Регіони з найбільшою кількістю унікального контенту

**Бізнес-ціль:** Визначити найактивніші ринки для інвестицій та партнерств.

**Застосовані операції:**
- `FILTER`: region IS NOT NULL, isAdult == 0, unique_titles >= 1000
- `JOIN`: title.akas + title.basics
- `GROUP BY`: region
- `countDistinct`: titles and types

**PySpark код:**
```python
q4_6 = df_akas \
    .filter(col("region").isNotNull()) \
    .join(df_basics, df_akas.titleId == df_basics.tconst) \
    .filter(col("isAdult") == 0) \
    .groupBy("region") \
    .agg(
        countDistinct("titleId").alias("unique_titles"),
        countDistinct("titleType").alias("content_types"),
        count("*").alias("total_entries")
    ) \
    .filter(col("unique_titles") >= 1000) \
    .withColumn("diversity_score", col("unique_titles") * col("content_types")) \
    .orderBy(col("unique_titles").desc()) \
    .limit(25)
```

**Аналіз плану виконання:**

```
Physical Plan:
├── TakeOrderedAndProject [limit=25]
│   └── Project [add diversity_score]
│       └── Filter [unique_titles >= 1000]
│           └── HashAggregate [final: countDistinct(titleId, titleType), count]
│               └── Exchange [HashPartitioning(region)]
│                   └── HashAggregate [partial: countDistinct]
│                       └── BroadcastHashJoin [titleId = tconst]
│                           ├── Filter [region IS NOT NULL]
│                           │   └── FileScan [akas] ← 36M rows
│                           └── BroadcastExchange
│                               └── Filter [isAdult = 0]
│                                   └── FileScan [basics]
```

**Оптимізації:**
1. **Low Cardinality GROUP BY**: ~200 регіонів
2. **countDistinct**: Дві різні distinct operations
3. **Post-Aggregation Calculation**: diversity_score після aggregation

**Особливості:**
- **Large akas Scan**: 36M рядків, але GROUP BY по region зменшує до ~200
- **Multiple countDistinct**: Потребує HyperLogLog або HashSet у пам'яті
- **High Selectivity Filter**: >= 1000 залишає ~50-70 регіонів

**Очікувана продуктивність:** Середня (20-30 сек)

---

## ОСОБА 5: Часовий аналіз та тренди

### Питання 5.1: Еволюція тривалості фільмів по десятиліттях

**Бізнес-ціль:** Аналіз історичних трендів тривалості фільмів для розуміння змін у індустрії.

**Застосовані операції:**
- `FILTER`: titleType, isAdult, startYear IS NOT NULL, runtimeMinutes IS NOT NULL, runtime BETWEEN 30-300, decade BETWEEN 1920-2020
- `GROUP BY`: decade
- `AGGREGATION`: count, avg, stddev, percentile_approx

**PySpark код:**
```python
q5_1 = df_basics \
    .filter(
        (col("titleType") == "movie") & 
        (col("isAdult") == 0) & 
        col("startYear").isNotNull() & 
        col("runtimeMinutes").isNotNull() &
        (col("runtimeMinutes").between(30, 300))
    ) \
    .withColumn("decade", (floor(col("startYear") / 10) * 10).cast("int")) \
    .filter(col("decade").between(1920, 2020)) \
    .groupBy("decade") \
    .agg(
        count("*").alias("movies_count"),
        round(avg("runtimeMinutes"), 1).alias("avg_runtime"),
        round(stddev("runtimeMinutes"), 1).alias("stddev_runtime"),
        percentile_approx("runtimeMinutes", 0.5).alias("median_runtime")
    ) \
    .orderBy("decade")
```

**Аналіз плану виконання:**

```
Physical Plan:
├── Sort [decade ASC]
│   └── HashAggregate [final: count, avg, stddev, percentile_approx]
│       └── Exchange [HashPartitioning(decade)]
│           └── HashAggregate [partial aggregates]
│               └── Filter [decade between 1920 and 2020]
│                   └── Project [calculate decade]
│                       └── Filter [all conditions on basics]
│                           └── FileScan [title.basics.tsv]
```

**Оптимізації:**
1. **Filter Pushdown**: Всі фільтри застосовуються при читанні
2. **Single Table**: Немає JOIN, тільки basics table
3. **Low Cardinality GROUP BY**: 11 значень (1920-2020 by 10)
4. **Statistical Functions**: avg, stddev efficient для numeric data

**Особливості:**
- **percentile_approx**: Approximate algorithm (t-digest), faster than exact
- **Range Filter**: 30-300 хв видаляє аномалії (99% фільмів)
- **Decade має малу cardinality**: Efficient shuffle

**Очікувана продуктивність:** Висока (5-10 сек, simple aggregation)

---

### Питання 5.2: Роки з найбільшою кількістю високорейтингових фільмів

**Бізнес-ціль:** Визначити найпродуктивніші роки кіноіндустрії для історичного аналізу.

**Застосовані операції:**
- `FILTER`: titleType, isAdult, startYear IS NOT NULL, averageRating >= 7.5, numVotes >= 1000, startYear BETWEEN 1990-2025
- `JOIN`: title.basics + title.ratings
- `GROUP BY`: startYear

**PySpark код:**
```python
q5_2 = df_basics \
    .filter((col("titleType") == "movie") & (col("isAdult") == 0) & col("startYear").isNotNull()) \
    .join(df_ratings, "tconst") \
    .filter((col("averageRating") >= 7.5) & (col("numVotes") >= 1000)) \
    .filter(col("startYear").between(1990, 2025)) \
    .groupBy("startYear") \
    .agg(
        count("*").alias("high_rated_count"),
        round(avg("averageRating"), 2).alias("avg_rating"),
        round(avg("numVotes"), 0).alias("avg_votes")
    ) \
    .orderBy(col("high_rated_count").desc()) \
    .limit(20)
```

**Аналіз плану виконання:**

```
Physical Plan:
├── TakeOrderedAndProject [limit=20, orderBy=high_rated_count DESC]
│   └── HashAggregate [final: (startYear)]
│       └── Exchange [HashPartitioning(startYear)]
│           └── HashAggregate [partial]
│               └── Filter [startYear between 1990 and 2025]
│                   └── BroadcastHashJoin [tconst]
│                       ├── Filter [basics conditions]
│                       └── BroadcastExchange
│                           └── Filter [rating >= 7.5 AND votes >= 1000]
│                               └── FileScan [ratings]
```

**Оптимізації:**
1. **Highly Selective Filters**: 7.5 rating + 1000 votes залишає ~10% рейтингів
2. **Narrow Year Range**: 36 років (1990-2025)
3. **Broadcast Join**: Filtered ratings broadcast-иться
4. **Low Cardinality GROUP BY**: 36 значень

**Очікувана продуктивність:** Висока (3-7 сек)

---

### Питання 5.3: Зростання популярності різних жанрів по декадах

**Бізнес-ціль:** Визначити тренди зміни переваг жанрів для стратегічного планування.

**Застосовані операції:**
- `FILTER`: titleType, isAdult, startYear IS NOT NULL, decade BETWEEN 1970-2020, growth_rate IS NOT NULL, movies_count >= 100
- `GROUP BY`: (decade, genre)
- `WINDOW FUNCTION`: lag() OVER (PARTITION BY genre ORDER BY decade)
- `EXPLODE`: genres

**PySpark код:**
```python
q5_3 = df_basics \
    .filter((col("titleType") == "movie") & (col("isAdult") == 0) & col("startYear").isNotNull()) \
    .withColumn("decade", (floor(col("startYear") / 10) * 10).cast("int")) \
    .filter(col("decade").between(1970, 2020)) \
    .withColumn("genre", explode(split(col("genres"), ","))) \
    .groupBy("decade", "genre") \
    .agg(count("*").alias("movies_count")) \
    .withColumn(
        "prev_decade_count",
        lag("movies_count").over(Window.partitionBy("genre").orderBy("decade"))
    ) \
    .withColumn(
        "growth_rate",
        when(col("prev_decade_count").isNotNull(),
             round((col("movies_count") - col("prev_decade_count")) / col("prev_decade_count") * 100, 1))
        .otherwise(lit(None))
    ) \
    .filter((col("growth_rate").isNotNull()) & (col("movies_count") >= 100)) \
    .select(...) \
    .orderBy(col("growth_rate").desc()) \
    .limit(30)
```

**Аналіз плану виконання:**

```
Physical Plan:
├── TakeOrderedAndProject [limit=30]
│   └── Filter [growth_rate IS NOT NULL AND movies_count >= 100]
│       └── Project [calculate growth_rate]
│           └── Window [lag(movies_count) OVER (PARTITION BY genre ORDER BY decade)]
│               └── Sort [genre, decade]
│                   └── Exchange [HashPartitioning(genre)]
│                       └── HashAggregate [final: (decade, genre)]
│                           └── Exchange [HashPartitioning(decade, genre)]
│                               └── HashAggregate [partial]
│                                   └── Generate [explode(genres)]
│                                       └── Filter & Project [decade calculation]
```

**Оптимізації:**
1. **lag() Window**: Ефективна функція для time-series
2. **Partition by genre**: ~20 жанрів, малі партиції
3. **Post-Window Filter**: Видаляє першу декаду кожного жанру (NULL growth_rate)

**Особливості:**
- **Explode Before GROUP BY**: Збільшує дані в ~2.5x
- **Double Shuffle**: Для GROUP BY та WINDOW
- **Growth Rate Calculation**: Requires non-NULL prev_decade_count
- **Time Series Analysis**: lag() дозволяє period-over-period comparison

**Очікувана продуктивність:** Середня (20-30 сек через explode + window)

---

### Питання 5.4: Розподіл фільмів по категоріям рейтингу в різні роки

**Бізнес-ціль:** Аналіз змін якості контенту протягом часу.

**Застосовані операції:**
- `FILTER`: titleType, isAdult, startYear IS NOT NULL, numVotes >= 5000, startYear BETWEEN 2000-2025
- `JOIN`: title.basics + title.ratings
- `GROUP BY`: (startYear, rating_category)
- `WINDOW FUNCTION`: sum() OVER (PARTITION BY startYear) для обчислення відсотків

**PySpark код:**
```python
q5_4 = df_basics \
    .filter((col("titleType") == "movie") & (col("isAdult") == 0) & col("startYear").isNotNull()) \
    .join(df_ratings, "tconst") \
    .filter((col("numVotes") >= 5000) & col("startYear").between(2000, 2025)) \
    .withColumn(
        "rating_category",
        when(col("averageRating") >= 8.0, "Excellent")
        .when(col("averageRating") >= 7.0, "Good")
        .when(col("averageRating") >= 6.0, "Average")
        .otherwise("Below Average")
    ) \
    .groupBy("startYear", "rating_category") \
    .agg(count("*").alias("movies_count")) \
    .withColumn(
        "pct_of_year",
        round(col("movies_count") / sum("movies_count").over(Window.partitionBy("startYear")) * 100, 1)
    ) \
    .select(...) \
    .orderBy("startYear", col("movies_count").desc())
```

**Аналіз плану виконання:**

```
Physical Plan:
├── Sort [startYear, movies_count DESC]
│   └── Project [calculate pct_of_year]
│       └── Window [sum(movies_count) OVER (PARTITION BY startYear)]
│           └── Sort [startYear]
│               └── Exchange [HashPartitioning(startYear)]
│                   └── HashAggregate [final: (startYear, rating_category)]
│                       └── Exchange [HashPartitioning(startYear, rating_category)]
│                           └── HashAggregate [partial]
│                               └── Project [categorize rating]
│                                   └── BroadcastHashJoin
│                                       ├── Filter [basics]
│                                       └── Filter [ratings - votes >= 5000]
```

**Оптимізації:**
1. **Selective Filters**: 5000 votes + year range зменшують dataset
2. **when/otherwise**: Category creation перед GROUP BY
3. **Window для відсотків**: Aggregating window function efficient
4. **Partitioning by startYear**: 26 партицій (2000-2025)

**Особливості:**
- **Two-Level Aggregation**: GROUP BY потім WINDOW SUM
- **Percentage Calculation**: Window SUM дає total per year
- **Category має low cardinality**: 4 значення

**Очікувана продуктивність:** Середня (15-20 сек)

---

### Питання 5.5: Аналіз віку популярних фільмів

**Бізнес-ціль:** Визначити, які періоди кіно залишаються популярними сьогодні.

**Застосовані операції:**
- `FILTER`: titleType, isAdult, startYear IS NOT NULL, numVotes >= 10000
- `JOIN`: title.basics + title.ratings
- `GROUP BY`: age_group
- `WINDOW FUNCTION`: row_number() для ранжування по популярності

**PySpark код:**
```python
q5_5 = df_basics \
    .filter((col("titleType") == "movie") & (col("isAdult") == 0) & col("startYear").isNotNull()) \
    .join(df_ratings, "tconst") \
    .filter(col("numVotes") >= 10000) \
    .withColumn("age", 2026 - col("startYear")) \
    .withColumn(
        "age_group",
        when(col("age") <= 5, "0-5 years")
        .when(col("age") <= 10, "6-10 years")
        .when(col("age") <= 20, "11-20 years")
        .when(col("age") <= 30, "21-30 years")
        .when(col("age") <= 50, "31-50 years")
        .otherwise("50+ years")
    ) \
    .groupBy("age_group") \
    .agg(
        count("*").alias("movies_count"),
        round(avg("averageRating"), 2).alias("avg_rating"),
        round(avg("numVotes"), 0).alias("avg_popularity"),
        max("numVotes").alias("max_votes")
    ) \
    .withColumn(
        "popularity_rank",
        row_number().over(Window.orderBy(col("avg_popularity").desc()))
    ) \
    .select(...)
```

**Аналіз плану виконання:**

```
Physical Plan:
├── Project [select columns]
│   └── Window [row_number() OVER (ORDER BY avg_popularity DESC)]
│       └── Sort [avg_popularity DESC]
│           └── Exchange [SinglePartition] ← Only 6 rows!
│               └── HashAggregate [final: groupBy=age_group]
│                   └── Exchange [HashPartitioning(age_group)]
│                       └── HashAggregate [partial]
│                           └── Project [calculate age and age_group]
│                               └── BroadcastHashJoin
│                                   ├── Filter [basics]
│                                   └── Filter [votes >= 10000]
```

**Оптимізації:**
1. **Highly Selective Filter**: 10000 votes → ~5000 фільмів
2. **Low Cardinality GROUP BY**: Тільки 6 age groups
3. **SinglePartition не costly**: Тільки 6 рядків збираються
4. **when Chain**: Age categorization efficient

**Очікувана продуктивність:** Висока (3-5 сек через малу кількість рядків)

---

### Питання 5.6: Кумулятивне зростання кіноіндустрії по декадам

**Бізнес-ціль:** Візуалізація історичного зростання обсягу виробництва фільмів.

**Застосовані операції:**
- `FILTER`: titleType, isAdult, startYear IS NOT NULL, numVotes >= 500, decade BETWEEN 1950-2020
- `JOIN`: title.basics + title.ratings
- `GROUP BY`: decade
- `WINDOW FUNCTION`: sum() OVER (ORDER BY decade ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)

**PySpark код:**
```python
q5_6 = df_basics \
    .filter((col("titleType") == "movie") & (col("isAdult") == 0) & col("startYear").isNotNull()) \
    .join(df_ratings, "tconst") \
    .filter(col("numVotes") >= 500) \
    .withColumn("decade", (floor(col("startYear") / 10) * 10).cast("int")) \
    .filter(col("decade").between(1950, 2020)) \
    .groupBy("decade") \
    .agg(
        count("*").alias("movies_count"),
        round(avg("averageRating"), 2).alias("avg_rating"),
        sum("numVotes").alias("total_votes")
    ) \
    .withColumn(
        "cumulative_movies",
        sum("movies_count").over(Window.orderBy("decade").rowsBetween(Window.unboundedPreceding, Window.currentRow))
    ) \
    .withColumn(
        "cumulative_votes",
        sum("total_votes").over(Window.orderBy("decade").rowsBetween(Window.unboundedPreceding, Window.currentRow))
    ) \
    .select(...) \
    .orderBy("decade")
```

**Аналіз плану виконання (formatted):**

```
Physical Plan (Formatted):
========================================================================================
├── Sort [decade ASC NULLS FIRST]
│   └── Project [*, cumulative_movies, cumulative_votes]
│       └── Window [sum(movies_count) OVER (ORDER BY decade ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)]
│           └── Window [sum(total_votes) OVER (ORDER BY decade ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)]
│               └── Sort [decade ASC NULLS FIRST]
│                   └── Exchange [SinglePartition] ← Cumulative requires sorted single partition!
│                       └── HashAggregate [final: (decade), agg={count, avg, sum}]
│                           └── Exchange [HashPartitioning(decade)]
│                               └── HashAggregate [partial]
│                                   └── Project [calculate decade]
│                                       └── BroadcastHashJoin [tconst]
│                                           ├── Filter [basics conditions]
│                                           └── Filter [votes >= 500]
========================================================================================

Execution Strategy:
-------------------
1. **Filter & Join Phase:**
   - Filter basics: titleType, isAdult, startYear
   - Filter ratings: numVotes >= 500
   - Broadcast Join on tconst
   - Result: ~50K movies

2. **Partial Aggregation:**
   - Local aggregation by decade (per partition)
   - Reduces data size significantly

3. **Shuffle & Final Aggregation:**
   - Repartition by decade (only 8 partitions: 1950-2020)
   - Final aggregation: count, avg, sum
   - Result: 8 rows

4. **Window Computation Phase:**
   - **Critical: SinglePartition Exchange!**
   - All 8 rows collected to one executor
   - Sort by decade (already mostly sorted)
   - Cumulative sum for movies_count
   - Cumulative sum for total_votes
   - Uses rowsBetween for running total

5. **Final Sort:**
   - Ensures output order by decade
   - Minimal cost (only 8 rows)

Optimizations Applied:
----------------------
✓ Filter Pushdown: All filters applied early
✓ Broadcast Join: Smaller ratings table broadcasted
✓ Two-Phase Aggregation: Partial then final
✓ Low Cardinality: Only 8 decades
✓ Single Partition OK: Only 8 rows collected

Performance Characteristics:
---------------------------
- Dataset Size: ~50K movies after filters
- GROUP BY Cardinality: 8 (very low)
- Window Input: Only 8 rows
- Cumulative Window: Ordered aggregating window (efficient)
- rowsBetween: Specifies frame for running total
- SinglePartition: Not problematic with 8 rows

Potential Issues:
-----------------
⚠ If decade range expanded: SinglePartition might become bottleneck
⚠ Window computation serialized: Cannot parallelize cumulative sum
✓ Current design OK: 8 rows fit in memory easily

Alternative Optimization (if needed):
------------------------------------
- Could use custom accumulator for cumulative calculation
- Or use RDD operations for manual cumulative sum
- But current approach optimal for small cardinality

Cost Breakdown:
--------------
- JOIN & Filter: 70% of time (~7-10 sec)
- GROUP BY & Shuffle: 20% (~2-3 sec)
- Window & Sort: 10% (~1 sec, only 8 rows)
- Total Expected: ~10-15 seconds
```

**Ключові моменти:**

1. **rowsBetween Window Frame:**
   ```scala
   Window.unboundedPreceding  // Start from first row
   Window.currentRow          // End at current row
   // = Running total from beginning to current position
   ```

2. **Cumulative Sum Pattern:**
   - Requires ORDER BY for frame definition
   - Processes rows in order, accumulating values
   - Each row sees sum of all previous + current

3. **SinglePartition Exchange:**
   - Необхідний для cumulative операцій
   - Всі дані збираються на одній ноді
   - Acceptable тільки для малих результатів GROUP BY

4. **Why Two Windows?**
   - Можна об'єднати в один window spec:
   ```python
   windowSpec = Window.orderBy("decade").rowsBetween(Window.unboundedPreceding, Window.currentRow)
   .withColumn("cumulative_movies", sum("movies_count").over(windowSpec))
   .withColumn("cumulative_votes", sum("total_votes").over(windowSpec))
   ```
   - Spark оптимізує в один прохід

**Очікувана продуктивність:** Висока (10-15 сек, незважаючи на window - тільки 8 рядків)

---

## Загальний аналіз та висновки

### Розподіл операцій по питаннях

| Операція | Кількість питань | Складність |
|----------|------------------|-----------|
| **FILTER** | 30/30 (100%) | Low-Medium |
| **JOIN** | 28/30 (93%) | Medium-High |
| **GROUP BY** | 27/30 (90%) | Medium |
| **WINDOW FUNCTION** | 13/30 (43%) | High |
| **EXPLODE** | 10/30 (33%) | Medium-High |
| **countDistinct** | 8/30 (27%) | Medium |

### Типи Window Functions використані:

1. **row_number()** - 6 питань (ранжування)
2. **lag()** - 2 питання (time series)
3. **min()/max()** - 3 питання (aggregate windows)
4. **avg()** - 2 питання (по партиціях)
5. **sum()** - 2 питання (cumulative та percentages)

### Найскладніші питання (за кількістю операцій):

1. **Питання 2.5** (Співпраці режисер-актор):
   - 6 JOIN (включаючи self-join на names)
   - EXPLODE
   - GROUP BY
   - WINDOW FUNCTION
   - Multiple FILTER
   - **Очікуваний час:** 2-5 хвилин

2. **Питання 2.4** (Ранжування акторів по декадах):
   - 4 JOIN
   - GROUP BY
   - WINDOW FUNCTION
   - FILTER
   - **Очікуваний час:** 60-120 секунд

3. **Питання 3.2** (Серіали з різницею рейтингів):
   - 4 JOIN
   - WINDOW FUNCTION (min/max)
   - GROUP BY
   - Multiple FILTER
   - **Очікуваний час:** 40-60 секунд

### Оптимізаційні патерни виявлені:

1. **Broadcast Join Pattern:**
   - Використовується у 25+ питаннях
   - Ефективно для таблиць <10GB або після фільтрації
   - Ratings, Names, filtered Basics - good candidates

2. **Two-Phase Aggregation:**
   - Partial aggregation локально
   - Shuffle
   - Final aggregation
   - Знижує network traffic

3. **Filter Pushdown:**
   - Catalyst optimizer застосовує фільтри якомога раніше
   - Зменшує обсяг даних для JOIN та аг регації

4. **Predicate Pushdown to Data Source:**
   - При читанні CSV може пропускати рядки
   - Ефективно для partition pruning

5. **Explode Optimization:**
   - Краще після JOIN та фільтрації
   - Зменшує inflation factor

### Рекомендації щодо виконання:

**Швидкі питання (<10 сек):**
- 1.1, 1.3, 1.6
- 3.3, 3.6
- 5.1, 5.2, 5.5

**Середні питання (10-30 сек):**
- 1.2, 1.4, 2.3
- 3.1, 3.4, 3.5
- 4.1, 4.2, 4.4, 4.5, 4.6
- 5.4, 5.6

**Повільні питання (30-60 сек):**
- 1.5, 2.1, 2.6
- 3.2
- 4.3
- 5.3

**Дуже повільні питання (>60 сек):**
- 2.2, 2.4, 2.5

### Покращення продуктивності:

1. **Для Питання 2.5:**
   ```python
   # Додати HINT для контролю broadcast
   .hint("broadcast", df_names)
   # Обмежити діапазон років
   .filter(col("startYear") >= 2000)
   ```

2. **Для питань з EXPLODE:**
   ```python
   # Кешувати результат JOIN перед explode
   df_joined = df_basics.join(df_ratings, "tconst").cache()
   df_exploded = df_joined.withColumn("genre", explode(...))
   ```

3. **Для питань з Window:**
   ```python
   # Збільшити executor memory
   spark.conf.set("spark.executor.memory", "8g")
   # Включити adaptive query execution
   spark.conf.set("spark.sql.adaptive.enabled", "true")
   ```

### Висновки:

Всі 30 питань покривають різні аспекти аналізу IMDb даних і демонструють використання ключових PySpark операцій. План виконання для кожного питання показує, як Catalyst optimizer оптимізує запити, але також виявляє потенційні bottlenecks (особливо у питаннях з множинними JOIN та window functions).
