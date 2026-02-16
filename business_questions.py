"""
Бізнес-питання для аналізу IMDb даних
Команда з 5 осіб: 30 питань загалом (6 питань на особу)

Кожен набір містить:
- Мінімум 3 питання з filter
- Мінімум 2 питання з join
- Мінімум 2 питання з group by
- Мінімум 2 питання з window functions
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from data_loader import *
import time

# Ініціалізація Spark
spark = SparkSession.builder \
    .appName("IMDb Business Questions") \
    .master("local[*]") \
    .config("spark.driver.memory", "4g") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .getOrCreate()

print("="*80)
print("Завантаження даних...")
print("="*80)

# Завантаження всіх датасетів
df_basics = load_title_basics(spark, "dataset")
df_ratings = load_title_ratings(spark, "dataset")
df_akas = load_title_akas(spark, "dataset")
df_crew = load_title_crew(spark, "dataset")
df_principals = load_title_principals(spark, "dataset")
df_names = load_name_basics(spark, "dataset")
df_episodes = load_title_episode(spark, "dataset")

print("\n[OK] Дані завантажено успішно\n")


def execute_query(query_number, person, description, dataframe, explain_mode="simple"):
    """Виконує запит та виводить результати з explain()"""
    print("="*80)
    print(f"ПИТАННЯ {query_number} (Особа {person})")
    print(f"{description}")
    print("="*80)
    
    start_time = time.time()
    
    # Показати результати
    print("\nРезультати:")
    print("-"*80)
    dataframe.show(20, truncate=False)
    
    execution_time = time.time() - start_time
    print(f"\nЧас виконання: {execution_time:.2f} секунд")
    
    # Показати план виконання
    print("\nПлан виконання (explain):")
    print("-"*80)
    dataframe.explain(mode=explain_mode)
    
    # Додаткові метрики
    print("\nДодаткові метрики:")
    print(f"- Кількість рядків у результаті: {dataframe.count()}")
    print(f"- Кількість партицій: {dataframe.rdd.getNumPartitions()}")
    
    print("\n" + "="*80 + "\n")
    return dataframe


# ============================================================================
# ОСОБА 1: Аналіз найкращих фільмів за різними критеріями
# ============================================================================

print("\n" + "="*80)
print("ОСОБА 1: Аналіз найкращих фільмів за різними критеріями")
print("="*80 + "\n")

# Питання 1.1: ТОП-10 найрейтинговіших фільмів з мінімум 10000 голосів (Filter + Join)
q1_1 = df_basics \
    .filter((col("titleType") == "movie") & (col("isAdult") == 0)) \
    .join(df_ratings, "tconst") \
    .filter(col("numVotes") >= 10000) \
    .select("tconst", "primaryTitle", "startYear", "genres", "averageRating", "numVotes") \
    .orderBy(col("averageRating").desc(), col("numVotes").desc()) \
    .limit(10)

execute_query(
    "1.1", "1",
    "ТОП-10 найрейтинговіших фільмів з мінімум 10000 голосів\n"
    "Операції: FILTER (titleType, isAdult, numVotes) + JOIN (basics + ratings)",
    q1_1
)

# Питання 1.2: Кількість фільмів по жанрах з середнім рейтингом (Group By + Join + Filter)
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

execute_query(
    "1.2", "1",
    "Кількість фільмів по жанрах з середнім рейтингом (мінімум 100 фільмів)\n"
    "Операції: GROUP BY (genre) + JOIN + FILTER + EXPLODE",
    q1_2
)

# Питання 1.3: Фільми доступні українською мовою з високим рейтингом (Filter + Join)
q1_3 = df_akas \
    .filter((col("region") == "UA") | (col("language") == "uk")) \
    .join(df_basics, df_akas.titleId == df_basics.tconst) \
    .filter((col("titleType") == "movie") & (col("isAdult") == 0)) \
    .join(df_ratings, "tconst") \
    .filter(col("averageRating") >= 7.0) \
    .select(
        "tconst", 
        df_basics.primaryTitle.alias("original_title"),
        df_akas.title.alias("ukrainian_title"),
        "startYear", 
        "averageRating", 
        "numVotes"
    ) \
    .orderBy(col("averageRating").desc()) \
    .limit(20)

execute_query(
    "1.3", "1",
    "Фільми доступні українською мовою з рейтингом >= 7.0\n"
    "Операції: FILTER (region, language, rating) + JOIN (3 таблиці)",
    q1_3
)

# Питання 1.4: Ранжування фільмів по рейтингу в кожному десятилітті (Window Function + Filter)
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
    .select("decade", "rank_in_decade", "primaryTitle", "startYear", "averageRating", "numVotes") \
    .orderBy("decade", "rank_in_decade")

execute_query(
    "1.4", "1",
    "ТОП-5 фільмів в кожному десятилітті\n"
    "Операції: WINDOW FUNCTION (row_number) + FILTER + Партиціонування",
    q1_4
)

# Питання 1.5: Різниця рейтингу фільму з середнім рейтингом його жанру (Window Function + Group By)
q1_5 = df_basics \
    .filter((col("titleType") == "movie") & (col("isAdult") == 0)) \
    .join(df_ratings, "tconst") \
    .filter(col("numVotes") >= 1000) \
    .withColumn("genre", explode(split(col("genres"), ","))) \
    .withColumn(
        "genre_avg_rating",
        avg("averageRating").over(Window.partitionBy("genre"))
    ) \
    .withColumn(
        "rating_diff",
        round(col("averageRating") - col("genre_avg_rating"), 2)
    ) \
    .filter(col("rating_diff") >= 1.5) \
    .select("primaryTitle", "genre", "averageRating", "genre_avg_rating", "rating_diff", "numVotes") \
    .orderBy(col("rating_diff").desc()) \
    .limit(20)

execute_query(
    "1.5", "1",
    "Фільми, які значно кращі за середній рейтинг свого жанру (різниця >= 1.5)\n"
    "Операції: WINDOW FUNCTION (avg) + FILTER + Партиціонування по жанру",
    q1_5
)

# Питання 1.6: Фільми 2020-х років з найбільшою динамікою популярності (Filter + Group By)
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

execute_query(
    "1.6", "1",
    "Динаміка якості та популярності фільмів 2020-2025 років\n"
    "Операції: FILTER (startYear range) + GROUP BY (startYear) + Агрегація",
    q1_6
)


# ============================================================================
# ОСОБА 2: Аналіз акторів та режисерів
# ============================================================================

print("\n" + "="*80)
print("ОСОБА 2: Аналіз акторів та режисерів")
print("="*80 + "\n")

# Питання 2.1: ТОП-10 режисерів з найбільшою кількістю високорейтингових фільмів (Join + Group By + Filter)
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

execute_query(
    "2.1", "2",
    "ТОП-10 режисерів з найбільшою кількістю високорейтингових фільмів (рейтинг >= 7.5)\n"
    "Операції: JOIN (4 таблиці) + GROUP BY (director) + FILTER + EXPLODE",
    q2_1
)

# Питання 2.2: Актори, які знімалися в найбільшій кількості жанрів (Join + Group By + Filter)
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

execute_query(
    "2.2", "2",
    "Актори з найбільшою різноманітністю жанрів (мінімум 10 фільмів)\n"
    "Операції: JOIN (3 таблиці) + GROUP BY + FILTER + countDistinct",
    q2_2
)

# Питання 2.3: Живі актори старше 70 років, які активні після 2010 (Filter + Join)
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

execute_query(
    "2.3", "2",
    "Активні актори старше 70 років (народилися до 1956, фільми після 2010)\n"
    "Операції: FILTER (birthYear, deathYear, startYear) + JOIN (3 таблиці)",
    q2_3
)

# Питання 2.4: Ранжування акторів по середньому рейтингу фільмів у кожному десятилітті (Window Function + Join)
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
        row_number().over(
            Window.partitionBy("decade")
            .orderBy(col("avg_rating").desc())
        )
    ) \
    .filter(col("rank_in_decade") <= 5) \
    .select("decade", "rank_in_decade", "primaryName", "avg_rating", "movies_count") \
    .orderBy("decade", "rank_in_decade")

execute_query(
    "2.4", "2",
    "ТОП-5 акторів по середньому рейтингу в кожному десятилітті (мінімум 3 фільми)\n"
    "Операції: WINDOW FUNCTION (row_number) + JOIN (4 таблиці) + GROUP BY",
    q2_4
)

# Питання 2.5: Співпраці режисер-актор з найвищими рейтингами (Window Function + Join + Filter)
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
    .groupBy("director_id", col("director.primaryName").alias("director_name"), 
             df_principals.nconst.alias("actor_id"), col("actor.primaryName").alias("actor_name")) \
    .agg(
        count("*").alias("collaborations"),
        round(avg("averageRating"), 2).alias("avg_rating")
    ) \
    .filter(col("collaborations") >= 3) \
    .withColumn(
        "rank",
        row_number().over(Window.orderBy(col("avg_rating").desc(), col("collaborations").desc()))
    ) \
    .filter(col("rank") <= 10) \
    .select("rank", "director_name", "actor_name", "collaborations", "avg_rating")

execute_query(
    "2.5", "2",
    "ТОП-10 співпраць режисер-актор з найвищими рейтингами (мінімум 3 фільми)\n"
    "Операції: WINDOW FUNCTION (row_number) + JOIN (5 таблиць) + FILTER + GROUP BY",
    q2_5,
    explain_mode="extended"
)

# Питання 2.6: Порівняння продуктивності акторів у різних жанрах (Group By + Filter)
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

execute_query(
    "2.6", "2",
    "Актори з найвищими рейтингами в окремих жанрах (мінімум 5 фільмів у жанрі)\n"
    "Операції: GROUP BY (actor, genre) + FILTER + JOIN (4 таблиці) + EXPLODE",
    q2_6
)


# ============================================================================
# ОСОБА 3: Аналіз серіалів та епізодів
# ============================================================================

print("\n" + "="*80)
print("ОСОБА 3: Аналіз серіалів та епізодів")
print("="*80 + "\n")

# Питання 3.1: ТОП-10 серіалів з найвищим середнім рейтингом епізодів (Join + Group By + Filter)
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

execute_query(
    "3.1", "3",
    "ТОП-10 серіалів з найвищим середнім рейтингом епізодів (мінімум 10 епізодів)\n"
    "Операції: JOIN (4 таблиці) + GROUP BY (series) + FILTER",
    q3_1
)

# Питання 3.2: Серіали з найбільшою різницею між найкращими та найгіршими епізодами (Window Function + Join)
q3_2 = df_episodes \
    .join(df_ratings.alias("episode_rating"), df_episodes.tconst == col("episode_rating.tconst")) \
    .join(df_basics.alias("series"), df_episodes.parentTconst == col("series.tconst")) \
    .join(df_basics.alias("episode"), df_episodes.tconst == col("episode.tconst")) \
    .filter(col("series.titleType") == "tvSeries") \
    .withColumn(
        "max_rating",
        max("episode_rating.averageRating").over(Window.partitionBy("parentTconst"))
    ) \
    .withColumn(
        "min_rating",
        min("episode_rating.averageRating").over(Window.partitionBy("parentTconst"))
    ) \
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

execute_query(
    "3.2", "3",
    "Серіали з найбільшою різницею якості епізодів (мінімум 20 епізодів)\n"
    "Операції: WINDOW FUNCTION (min, max) + JOIN (4 таблиці) + GROUP BY",
    q3_2
)

# Питання 3.3: Епізоди серіалів доступні українською (Filter + Join)
q3_3 = df_akas \
    .filter((col("region") == "UA") | (col("language") == "uk")) \
    .join(df_episodes, df_akas.titleId == df_episodes.tconst) \
    .join(df_basics.alias("series"), df_episodes.parentTconst == col("series.tconst")) \
    .filter(col("series.titleType") == "tvSeries") \
    .join(df_ratings, df_episodes.tconst == df_ratings.tconst) \
    .filter(col("averageRating") >= 8.0) \
    .select(
        col("series.primaryTitle").alias("series_title"),
        "seasonNumber",
        "episodeNumber",
        df_akas.title.alias("ukrainian_title"),
        "averageRating",
        "numVotes"
    ) \
    .orderBy(col("averageRating").desc()) \
    .limit(20)

execute_query(
    "3.3", "3",
    "Високорейтингові епізоди серіалів українською (рейтинг >= 8.0)\n"
    "Операції: FILTER (region, language, rating) + JOIN (4 таблиці)",
    q3_3
)

# Питання 3.4: Динаміка рейтингів по сезонах для кожного серіалу (Window Function + Group By + Filter)
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
        lag("season_avg_rating").over(
            Window.partitionBy("parentTconst").orderBy("seasonNumber")
        )
    ) \
    .withColumn(
        "rating_change",
        when(col("prev_season_rating").isNotNull(), 
             round(col("season_avg_rating") - col("prev_season_rating"), 2))
        .otherwise(lit(None))
    ) \
    .filter(col("seasonNumber") <= 10) \
    .select("primaryTitle", "seasonNumber", "season_avg_rating", "rating_change", "episodes_in_season") \
    .orderBy("primaryTitle", "seasonNumber") \
    .limit(50)

execute_query(
    "3.4", "3",
    "Динаміка зміни рейтингів по сезонах (перші 10 сезонів)\n"
    "Операції: WINDOW FUNCTION (lag) + GROUP BY + FILTER + Партиціонування",
    q3_4
)

# Питання 3.5: Серіали з найкращими фінальними сезонами (Window Function + Filter)
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

execute_query(
    "3.5", "3",
    "Серіали з найкращими фінальними сезонами (мінімум 3 сезони, 5 епізодів у фіналі)\n"
    "Операції: WINDOW FUNCTION (max) + FILTER + GROUP BY + Партиціонування",
    q3_5
)

# Питання 3.6: Порівняння популярності різних типів серіалів (Group By + Filter + Join)
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

execute_query(
    "3.6", "3",
    "Порівняння популярності типів серіалів по декадам (з 1990-х)\n"
    "Операції: GROUP BY (decade, titleType) + FILTER + JOIN + Агрегація",
    q3_6
)


# ============================================================================
# ОСОБА 4: Географічний та мовний аналіз
# ============================================================================

print("\n" + "="*80)
print("ОСОБА 4: Географічний та мовний аналіз")
print("="*80 + "\n")

# Питання 4.1: Кількість локалізацій для найпопулярніших фільмів (Join + Group By + Filter)
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

execute_query(
    "4.1", "4",
    "Найбільш локалізовані фільми (мінімум 50000 голосів)\n"
    "Операції: JOIN (3 таблиці) + GROUP BY + FILTER + countDistinct",
    q4_1
)

# Питання 4.2: Найпопулярніші жанри в різних країнах (Join + Group By + Filter)
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
        row_number().over(
            Window.partitionBy("region")
            .orderBy(col("movies_count").desc())
        )
    ) \
    .filter(col("rank_in_region") <= 3) \
    .select("region", "rank_in_region", "genre", "movies_count", "avg_rating") \
    .orderBy("region", "rank_in_region")

execute_query(
    "4.2", "4",
    "ТОП-3 жанри в різних країнах (US, GB, FR, DE, JP, KR, IN, BR, UA)\n"
    "Операції: JOIN (3 таблиці) + GROUP BY + FILTER + WINDOW FUNCTION (row_number)",
    q4_2
)

# Питання 4.3: Фільми з найбільшою кількістю альтернативних назв (Filter + Join + Group By)
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

execute_query(
    "4.3", "4",
    "Фільми з найбільшою кількістю альтернативних назв (мінімум 10 назв)\n"
    "Операції: JOIN + GROUP BY + FILTER (різні назви)",
    q4_3
)

# Питання 4.4: Середній рейтинг фільмів по мовах (Window Function + Join + Group By)
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
    .select("rating_rank", "language", "movies_count", "avg_rating", "total_votes") \
    .orderBy("rating_rank") \
    .limit(20)

execute_query(
    "4.4", "4",
    "ТОП-20 мов за середнім рейтингом фільмів (мінімум 50 фільмів)\n"
    "Операції: WINDOW FUNCTION (row_number) + JOIN + GROUP BY + FILTER",
    q4_4
)

# Питання 4.5: Порівняння локалізації фільмів по десятиліттях (Filter + Join + Group By)
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

execute_query(
    "4.5", "4",
    "Динаміка локалізації фільмів по декадам (з 1950-х)\n"
    "Операції: FILTER + JOIN + GROUP BY (nested) + Агрегація",
    q4_5
)

# Питання 4.6: Регіони з найбільшою кількістю унікального контенту (Filter + Group By)
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

execute_query(
    "4.6", "4",
    "ТОП-25 регіонів за кількістю унікального контенту (мінімум 1000 тайтлів)\n"
    "Операції: GROUP BY (region) + FILTER + countDistinct + Агрегація",
    q4_6
)


# ============================================================================
# ОСОБА 5: Часовий аналіз та тренди
# ============================================================================

print("\n" + "="*80)
print("ОСОБА 5: Часовий аналіз та тренди")
print("="*80 + "\n")

# Питання 5.1: Еволюція тривалості фільмів по десятиліттях (Filter + Group By)
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

execute_query(
    "5.1", "5",
    "Еволюція тривалості фільмів по декадам (1920-2020, 30-300 хв)\n"
    "Операції: FILTER (range checks) + GROUP BY (decade) + Статистичні агрегації",
    q5_1
)

# Питання 5.2: Року з найбільшою кількістю високорейтингових фільмів (Join + Filter + Group By)
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

execute_query(
    "5.2", "5",
    "Роки з найбільшою кількістю високоякісних фільмів (1990-2025, рейтинг >= 7.5)\n"
    "Операції: JOIN + FILTER (multiple conditions) + GROUP BY (year)",
    q5_2
)

# Питання 5.3: Зростання популярності різних жанрів по декадах (Window Function + Group By + Filter)
q5_3 = df_basics \
    .filter((col("titleType") == "movie") & (col("isAdult") == 0) & col("startYear").isNotNull()) \
    .withColumn("decade", (floor(col("startYear") / 10) * 10).cast("int")) \
    .filter(col("decade").between(1970, 2020)) \
    .withColumn("genre", explode(split(col("genres"), ","))) \
    .groupBy("decade", "genre") \
    .agg(count("*").alias("movies_count")) \
    .withColumn(
        "prev_decade_count",
        lag("movies_count").over(
            Window.partitionBy("genre").orderBy("decade")
        )
    ) \
    .withColumn(
        "growth_rate",
        when(col("prev_decade_count").isNotNull(),
             round((col("movies_count") - col("prev_decade_count")) / col("prev_decade_count") * 100, 1))
        .otherwise(lit(None))
    ) \
    .filter((col("growth_rate").isNotNull()) & (col("movies_count") >= 100)) \
    .select("decade", "genre", "movies_count", "prev_decade_count", "growth_rate") \
    .orderBy(col("growth_rate").desc()) \
    .limit(30)

execute_query(
    "5.3", "5",
    "Жанри з найбільшим темпом зростання по декадах (1970-2020)\n"
    "Операції: WINDOW FUNCTION (lag) + GROUP BY + FILTER + EXPLODE",
    q5_3
)

# Питання 5.4: Сезонність випуску високорейтингових фільмів (якщо є місяць у даних) - альтернативно: розподіл по рокам з рангуванням (Window Function + Join)
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
    .select("startYear", "rating_category", "movies_count", "pct_of_year") \
    .orderBy("startYear", col("movies_count").desc())

execute_query(
    "5.4", "5",
    "Розподіл фільмів по категоріям рейтингу в різні роки (2000-2025)\n"
    "Операції: WINDOW FUNCTION (sum partition) + JOIN + FILTER + GROUP BY",
    q5_4
)

# Питання 5.5: Старіння контенту - коли були створені найбільш популярні сьогодні фільми (Window Function + Filter + Join)
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
    .select("popularity_rank", "age_group", "movies_count", "avg_rating", "avg_popularity", "max_votes") \
    .orderBy("popularity_rank")

execute_query(
    "5.5", "5",
    "Аналіз віку популярних фільмів (мінімум 10000 голосів)\n"
    "Операції: WINDOW FUNCTION (row_number) + FILTER + JOIN + GROUP BY + when",
    q5_5
)

# Питання 5.6: Порівняння продуктивності десятиліть (cumulative) (Window Function + Group By + Filter)
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
    .select("decade", "movies_count", "cumulative_movies", "avg_rating", "total_votes", "cumulative_votes") \
    .orderBy("decade")

execute_query(
    "5.6", "5",
    "Кумулятивне зростання кіноіндустрії по декадам (1950-2020)\n"
    "Операції: WINDOW FUNCTION (sum cumulative) + GROUP BY + FILTER + rowsBetween",
    q5_6,
    explain_mode="formatted"
)


# ============================================================================
# Завершення
# ============================================================================

print("\n" + "="*80)
print("АНАЛІЗ ЗАВЕРШЕНО")
print("="*80)
print(f"\nВсього виконано: 30 бізнес-питань (6 питань × 5 осіб)")
print("\nРозподіл по операціях:")
print("- Filter: використано у всіх 30 питаннях")
print("- Join: використано у 24+ питаннях")
print("- Group By: використано у 26+ питаннях")
print("- Window Functions: використано у 12+ питаннях")
print("\nТипи аналізу:")
print("- Особа 1: Найкращі фільми за різними критеріями")
print("- Особа 2: Аналіз акторів та режисерів")
print("- Особа 3: Аналіз серіалів та епізодів")
print("- Особа 4: Географічний та мовний аналіз")
print("- Особа 5: Часовий аналіз та тренди")
print("="*80)

spark.stop()
