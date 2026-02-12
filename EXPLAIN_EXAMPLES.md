# Приклади explain() для різних режимів

## Режими explain()

PySpark підтримує 4 режими для методу `.explain()`:

1. **simple** (за замовчуванням) - базовий physical plan
2. **extended** - physical + logical + optimized logical plans
3. **formatted** - форматований вивід з додатковими деталями
4. **cost** - включає оцінки вартості операцій

## Приклад 1: Simple Mode

### Запит:
```python
df_basics \
    .filter(col("titleType") == "movie") \
    .join(df_ratings, "tconst") \
    .filter(col("averageRating") >= 8.0) \
    .select("primaryTitle", "averageRating") \
    .explain(mode="simple")
```

### Вивід (simple):
```
== Physical Plan ==
Project [primaryTitle#10, averageRating#45]
+- Filter (averageRating#45 >= 8.0)
   +- BroadcastHashJoin [tconst#9], [tconst#44], Inner, BuildRight, false
      :- Project [tconst#9, primaryTitle#10]
      :  +- Filter ((titleType#11 = movie) AND isnotnull(tconst#9))
      :     +- FileScan csv [tconst#9,primaryTitle#10,titleType#11] 
      +- BroadcastExchange HashedRelationBroadcastMode(tconst#44)
         +- Filter (isnotnull(averageRating#45) AND (averageRating#45 >= 8.0))
            +- FileScan csv [tconst#44,averageRating#45]
```

### Інтерпретація:
- ✅ **BroadcastHashJoin**: ratings таблиця broadcast-иться (ефективно)
- ✅ **Filter Pushdown**: Фільтри застосовуються перед JOIN
- ✅ **Project**: Тільки потрібні колонки читаються

---

## Приклад 2: Extended Mode

### Запит:
```python
df_basics \
    .filter(col("titleType") == "movie") \
    .groupBy("startYear") \
    .agg(count("*").alias("movies_count")) \
    .explain(mode="extended")
```

### Вивід (extended):
```
== Parsed Logical Plan ==
'Aggregate ['startYear], ['startYear, count(1) AS movies_count#123L]
+- Filter (titleType#11 = movie)
   +- Relation [tconst#9,primaryTitle#10,titleType#11,startYear#13] csv

== Analyzed Logical Plan ==
startYear: int, movies_count: bigint
Aggregate [startYear#13], [startYear#13, count(1) AS movies_count#123L]
+- Filter (titleType#11 = movie)
   +- Relation [tconst#9,primaryTitle#10,titleType#11,startYear#13] csv

== Optimized Logical Plan ==
Aggregate [startYear#13], [startYear#13, count(1) AS movies_count#123L]
+- Project [titleType#11, startYear#13]
   +- Filter (isnotnull(titleType#11) AND (titleType#11 = movie))
      +- Relation [tconst#9,primaryTitle#10,titleType#11,startYear#13] csv

== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- HashAggregate [startYear#13], [startYear#13, count(1) AS movies_count#123L]
   +- Exchange hashpartitioning(startYear#13, 200)
      +- HashAggregate [startYear#13], [startYear#13, count(1) AS count#126L]
         +- Project [titleType#11, startYear#13]
            +- Filter (isnotnull(titleType#11) AND (titleType#11 = movie))
               +- FileScan csv [tconst#9,primaryTitle#10,titleType#11,startYear#13]
```

### Інтерпретація:

**Parsed Logical Plan:**
- Початкове представлення запиту

**Analyzed Logical Plan:**
- Після type checking та resolution

**Optimized Logical Plan:**
- Після Catalyst optimizer:
  - ✅ Додано isnotnull перевірки
  - ✅ Project зменшує кількість колонок

**Physical Plan:**
- ✅ **Two-Phase Aggregation**: Partial (local) + Final (after shuffle)
- ✅ **HashPartitioning**: Дані перерозподіляються по startYear
- ✅ **AdaptiveSparkPlan**: Adaptive Query Execution (AQE) увімкнено

---

## Приклад 3: Window Function

### Запит:
```python
df_basics \
    .join(df_ratings, "tconst") \
    .withColumn("decade", (floor(col("startYear") / 10) * 10)) \
    .withColumn(
        "rank",
        row_number().over(Window.partitionBy("decade").orderBy(col("averageRating").desc()))
    ) \
    .filter(col("rank") <= 3) \
    .explain(mode="simple")
```

### Вивід:
```
== Physical Plan ==
Filter (rank#234 <= 3)
+- Window [row_number() OVER (PARTITION BY decade#127 ORDER BY averageRating#45 DESC)], [decade#127], [averageRating#45 DESC]
   +- Sort [decade#127 ASC NULLS FIRST, averageRating#45 DESC NULLS LAST]
      +- Exchange hashpartitioning(decade#127, 200)
         +- Project [*, floor((cast(startYear#13 / 10) * 10)) AS decade#127]
            +- BroadcastHashJoin [tconst#9], [tconst#44], Inner
               :- FileScan csv [...]
               +- BroadcastExchange
                  +- FileScan csv [...]
```

### Інтерпретація:
- 🔄 **Exchange (Shuffle)**: Дані перерозподіляються по decade
- 📊 **Sort**: Сортування в межах кожної партиції (decade)
- 🪟 **Window**: row_number() застосовується по відсортованих партиціях
- ✅ **Filter After Window**: rank <= 3 застосовується після обчислення

---

## Приклад 4: Formatted Mode (Spark 3.0+)

### Запит:
```python
df_basics \
    .groupBy("titleType") \
    .agg(count("*").alias("count")) \
    .explain(mode="formatted")
```

### Вивід (formatted):
```
== Physical Plan ==
AdaptiveSparkPlan (1)
+- HashAggregate (6)
   +- Exchange (5)
      +- HashAggregate (4)
         +- Project (3)
            +- FileScan (2)


(1) AdaptiveSparkPlan
Output [2]: [titleType#11, count#145L]
Arguments: isFinalPlan=false

(2) FileScan csv
Output [1]: [titleType#11]
Batched: false
Location: InMemoryFileIndex [file:///E:/BBD/dataset/title.basics.tsv]
PushedFilters: [IsNotNull(titleType)]
ReadSchema: struct<titleType:string>

(3) Project
Output [1]: [titleType#11]
Input [1]: [titleType#11]

(4) HashAggregate
Input [1]: [titleType#11]
Keys [1]: [titleType#11]
Functions [1]: [partial_count(1)]
Aggregate Attributes [1]: [count#149L]
Results [2]: [titleType#11, count#149L]

(5) Exchange
Input [2]: [titleType#11, count#149L]
Arguments: hashpartitioning(titleType#11, 200), ENSURE_REQUIREMENTS, [plan_id=123]

(6) HashAggregate
Input [2]: [titleType#11, count#149L]
Keys [1]: [titleType#11]
Functions [1]: [count(1)]
Aggregate Attributes [1]: [count(1)#144L]
Results [2]: [titleType#11, count(1)#144L AS count#145L]
```

### Інтерпретація:
- 📋 **Деталізована структура**: Кожна операція пронумерована
- 📊 **Input/Output Schema**: Видно які колонки на вході/виході
- 📍 **Location**: Видно шлях до файлів
- 🔍 **PushedFilters**: Які фільтри застосовані при читанні
- 📐 **Arguments**: Параметри кожної операції

---

## Типові патерни оптимізації

### 1. Broadcast Join (Good! ✅)
```
BroadcastHashJoin [join_key], Inner, BuildRight
+- FileScan large_table
+- BroadcastExchange
   +- FileScan small_table (<10MB)
```
**Переваги:** Немає shuffle для великої таблиці

### 2. Sort Merge Join (Може бути costly ⚠️)
```
SortMergeJoin [join_key], Inner
:- Sort [join_key ASC]
:  +- Exchange hashpartitioning(join_key)
:     +- FileScan table1
+- Sort [join_key ASC]
   +- Exchange hashpartitioning(join_key)
      +- FileScan table2
```
**Недоліки:** Подвійний shuffle + подвійний sort

### 3. Two-Phase Aggregation (Good! ✅)
```
HashAggregate [key], [final aggregates]
+- Exchange hashpartitioning(key)
   +- HashAggregate [key], [partial aggregates]
```
**Переваги:** Часткова агрегація зменшує shuffle data

### 4. Filter Pushdown (Excellent! ✅✅)
```
FileScan csv
PushedFilters: [IsNotNull(col), GreaterThan(col, value)]
```
**Переваги:** Фільтрація при читанні файлу

### 5. Window з Shuffle (Необхідний для партиціонування ⚠️)
```
Window [func() OVER (PARTITION BY key)]
+- Sort [key, order_key]
   +- Exchange hashpartitioning(key)
```
**Note:** Shuffle необхідний для групування по ключу

### 6. SinglePartition Exchange (BAD для великих даних ❌)
```
Exchange SinglePartition
```
**Проблема:** Всі дані збираються на одній ноді!
**Коли використовується:** Window без PARTITION BY, orderBy без limit

---

## Як читати кількість партицій

### У Physical Plan:
```
Exchange hashpartitioning(key, 200)
                            ^
                            Кількість партицій (за замовчуванням)
```

### Налаштування:
```python
# Зміна кількості shuffle партицій
spark.conf.set("spark.sql.shuffle.partitions", "100")

# Для малих датасетів - зменшити
spark.conf.set("spark.sql.shuffle.partitions", "20")

# Для великих - збільшити
spark.conf.set("spark.sql.shuffle.partitions", "400")
```

---

## Оцінка вартості операцій

### Відносна вартість (час виконання):
1. **FileScan**: Low (якщо є filter pushdown)
2. **Project**: Very Low
3. **Filter (in-memory)**: Low
4. **BroadcastJoin**: Medium (залежить від розміру broadcast table)
5. **HashAggregate (no shuffle)**: Medium
6. **Exchange (Shuffle)**: **HIGH** ⚠️
7. **Sort**: High (залежить від розміру)
8. **SortMergeJoin**: Very High (shuffle + sort × 2)
9. **Window**: High (shuffle + sort + window computation)
10. **Explode**: Medium-High (збільшує кількість рядків)

### Ключові метрики:
- **Shuffle Read/Write**: Скільки даних передається мережею
- **Records Read**: Скільки рядків прочитано з файлів
- **Records Written**: Скільки записано (для intermediate results)
- **Peak Memory**: Максимальне використання пам'яті

---

## Практичні поради

### 1. Завжди перевіряйте explain перед запуском на великих даних
```python
# Спочатку на sample
df.sample(0.01).filter(...).explain()

# Потім на повних даних
df.filter(...).explain()
```

### 2. Шукайте SinglePartition exchanges
```python
# Погано
.withColumn("rank", row_number().over(Window.orderBy("col")))

# Краще - додайте PARTITION BY
.withColumn("rank", row_number().over(Window.partitionBy("category").orderBy("col")))
```

### 3. Перевіряйте broadcast joins
```python
# Якщо таблиця занадто велика для broadcast:
== Physical Plan ==
SortMergeJoin  # <- Повільніше

# Можна примусово встановити broadcast
df1.join(broadcast(df2), "key")
```

### 4. Оптимізуйте кількість shuffle партицій
```python
# Для малих групувань
spark.conf.set("spark.sql.shuffle.partitions", "20")

# Для великих
spark.conf.set("spark.sql.shuffle.partitions", "400")

# Або використайте AQE (автоматична оптимізація)
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
```

---

## Висновок

explain() - потужний інструмент для:
- 🔍 Розуміння як Spark виконує запит
- 🚀 Виявлення bottlenecks
- 💡 Оптимізації performance
- 📊 Валідації оптимізацій Catalyst

**Рекомендація:** Завжди аналізуйте explain() для складних запитів перед production!
