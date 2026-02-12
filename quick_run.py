"""
Швидкий запуск окремих бізнес-питань
Використовуйте для швидкого тестування без запуску всіх 30 питань
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from data_loader import *
import sys

# Ініціалізація Spark
spark = SparkSession.builder \
    .appName("IMDb Quick Test") \
    .master("local[*]") \
    .config("spark.driver.memory", "4g") \
    .config("spark.sql.adaptive.enabled", "true") \
    .getOrCreate()

print("="*80)
print("Завантаження даних...")
print("="*80)

# Завантаження датасетів
df_basics = load_title_basics(spark, "dataset")
df_ratings = load_title_ratings(spark, "dataset")
df_akas = load_title_akas(spark, "dataset")
df_crew = load_title_crew(spark, "dataset")
df_principals = load_title_principals(spark, "dataset")
df_names = load_name_basics(spark, "dataset")
df_episodes = load_title_episode(spark, "dataset")

print("[OK] Дані завантажено\n")


def run_query(query_num):
    """Запустити окреме питання за номером"""
    
    print("="*80)
    print(f"Виконання питання {query_num}")
    print("="*80 + "\n")
    
    # ОСОБА 1
    if query_num == "1.1":
        print("Питання 1.1: ТОП-10 найрейтинговіших фільмів з мінімум 10000 голосів\n")
        result = df_basics \
            .filter((col("titleType") == "movie") & (col("isAdult") == 0)) \
            .join(df_ratings, "tconst") \
            .filter(col("numVotes") >= 10000) \
            .select("tconst", "primaryTitle", "startYear", "genres", "averageRating", "numVotes") \
            .orderBy(col("averageRating").desc(), col("numVotes").desc()) \
            .limit(10)
    
    elif query_num == "1.2":
        print("Питання 1.2: Кількість фільмів по жанрах з середнім рейтингом\n")
        result = df_basics \
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
    
    elif query_num == "1.3":
        print("Питання 1.3: Фільми доступні українською мовою з високим рейтингом\n")
        result = df_akas \
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
    
    elif query_num == "1.4":
        print("Питання 1.4: ТОП-5 фільмів в кожному десятилітті\n")
        result = df_basics \
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
    
    elif query_num == "1.5":
        print("Питання 1.5: Фільми значно кращі за середній рейтинг свого жанру\n")
        result = df_basics \
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
    
    elif query_num == "1.6":
        print("Питання 1.6: Динаміка якості та популярності фільмів 2020-2025\n")
        result = df_basics \
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
    
    # ОСОБА 2
    elif query_num == "2.1":
        print("Питання 2.1: ТОП-10 режисерів з високорейтинговими фільмами\n")
        result = df_crew \
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
    
    elif query_num == "5.1":
        print("Питання 5.1: Еволюція тривалості фільмів по декадам\n")
        result = df_basics \
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
    
    else:
        print(f"[ERROR] Питання {query_num} не знайдено!")
        print("\nДоступні питання для швидкого тестування:")
        print("  1.1 - ТОП-10 фільмів")
        print("  1.2 - Статистика по жанрах")
        print("  1.3 - Фільми українською")
        print("  1.4 - ТОП-5 по декадах (Window)")
        print("  1.5 - Фільми вище середнього жанру (Window)")
        print("  1.6 - Динаміка 2020-х")
        print("  2.1 - ТОП-10 режисерів")
        print("  5.1 - Еволюція тривалості")
        return
    
    # Виконання та виведення
    print("Результати:")
    print("-"*80)
    result.show(30, truncate=False)
    
    print(f"\nКількість рядків: {result.count()}")
    
    print("\nПлан виконання:")
    print("-"*80)
    result.explain(mode="simple")
    
    print("\n" + "="*80)


# Головна функція
if __name__ == "__main__":
    if len(sys.argv) > 1:
        query_num = sys.argv[1]
        run_query(query_num)
    else:
        print("="*80)
        print("ШВИДКИЙ ЗАПУСК ОКРЕМИХ ПИТАНЬ")
        print("="*80)
        print("\nВикористання:")
        print("  python quick_run.py <номер_питання>")
        print("\nПриклади:")
        print("  python quick_run.py 1.1")
        print("  python quick_run.py 1.4")
        print("  python quick_run.py 2.1")
        print("\nДоступні питання:")
        print("  ОСОБА 1: 1.1, 1.2, 1.3, 1.4, 1.5, 1.6")
        print("  ОСОБА 2: 2.1")
        print("  ОСОБА 5: 5.1")
        print("\nДля всіх 30 питань використовуйте:")
        print("  python business_questions.py")
        print("="*80)
    
    spark.stop()
