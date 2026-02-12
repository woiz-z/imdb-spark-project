#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Попередній аналіз набору даних IMDb
"""

import os
import csv

try:
    import pandas as pd
    PANDAS_AVAILABLE = True
except (ImportError, ValueError) as e:
    print(f"⚠️  Pandas недоступний: {e}")
    print("   Використовуємо базові методи Python\n")
    PANDAS_AVAILABLE = False

print("=" * 80)
print("ПОПЕРЕДНІЙ АНАЛІЗ НАБОРУ ДАНИХ IMDb")
print("=" * 80)

dataset_dir = r"e:\BBD\dataset"

# Словник з описом файлів
files_info = {
    'name.basics.tsv': 'Інформація про людей (актори, режисери, письменники)',
    'title.akas.tsv': 'Альтернативні назви фільмів/серіалів (різні мови/регіони)',
    'title.basics.tsv': 'Основна інформація про фільми/серіали',
    'title.crew.tsv': 'Інформація про команду (режисери, сценаристи)',
    'title.episode.tsv': 'Інформація про епізоди серіалів',
    'title.principals.tsv': 'Головні учасники (акторський склад та команда)',
    'title.ratings.tsv': 'Рейтинги фільмів/серіалів'
}

print("\n1. СТРУКТУРА ФАЙЛІВ:\n")
for filename, description in files_info.items():
    filepath = os.path.join(dataset_dir, filename)
    size_mb = os.path.getsize(filepath) / (1024 * 1024)
    print(f"📁 {filename}")
    print(f"   Опис: {description}")
    print(f"   Розмір: {size_mb:.2f} MB")
    
    # Читаємо перші кілька рядків для визначення структури
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            lines = [next(f) for _ in range(3)]
            header = lines[0].strip().split('\t')
            first_row = lines[1].strip().split('\t')
            print(f"   Стовпці ({len(header)}): {', '.join(header[:6])}" + 
                  ("..." if len(header) > 6 else ""))
            print(f"   Приклад першого рядка:")
            for i, col in enumerate(header[:4]):
                value = first_row[i] if i < len(first_row) else "N/A"
                if len(value) > 50:
                    value = value[:47] + "..."
                print(f"      {col}: {value}")
    except Exception as e:
        print(f"   Помилка читання: {e}")
    print()

print("\n2. АНАЛІЗ КОЖНОГО ФАЙЛУ:\n")

# name.basics.tsv - Люди
print("📊 name.basics.tsv - ІНФОРМАЦІЯ ПРО ЛЮДЕЙ")
print("-" * 60)
print(f"• Стовпці: nconst, primaryName, birthYear, deathYear, primaryProfession, knownForTitles")
print(f"• Корисна інформація:")
print(f"  - nconst: унікальний ідентифікатор особи")
print(f"  - primaryName: ім'я")
print(f"  - birthYear/deathYear: роки народження/смерті")
print(f"  - primaryProfession: основна професія")
print(f"  - knownForTitles: відомі роботи")
print(f"• Можлива корисність: висока (для аналізу акторів, режисерів)")
print()

# title.basics.tsv - Основна інформація про фільми
print("📊 title.basics.tsv - ОСНОВНА ІНФОРМАЦІЯ ПРО ФІЛЬМИ/СЕРІАЛИ")
print("-" * 60)
print(f"• Стовпці: tconst, titleType, primaryTitle, originalTitle, isAdult, startYear, endYear, runtimeMinutes, genres")
print(f"• Корисна інформація:")
print(f"  - tconst: унікальний ідентифікатор")
print(f"  - titleType: тип (movie, tvSeries, short, etc.)")
print(f"  - primaryTitle: основна назва")
print(f"  - isAdult: маркер дорослого контенту")
print(f"  - startYear/endYear: роки випуску")
print(f"  - runtimeMinutes: тривалість")
print(f"  - genres: жанри")
print(f"• Можлива корисність: ДУЖЕ ВИСОКА (центральна таблиця)")
print()

# title.ratings.tsv - Рейтинги
print("📊 title.ratings.tsv - РЕЙТИНГИ")
print("-" * 60)
try:
    filepath = os.path.join(dataset_dir, 'title.ratings.tsv')
    with open(filepath, 'r', encoding='utf-8') as f:
        lines = f.readlines()
        total_lines = len(lines) - 1  # Мінус заголовок
        
        # Простий аналіз
        ratings = []
        for i, line in enumerate(lines[1:]):  # Пропускаємо заголовок
            if i >= 10000:  # Обмежуємо для швидкості
                break
            parts = line.strip().split('\t')
            if len(parts) >= 2:
                try:
                    ratings.append(float(parts[1]))
                except:
                    pass
        
        print(f"• Стовпці: tconst, averageRating, numVotes")
        print(f"• Корисна інформація:")
        print(f"  - tconst: ідентифікатор фільму")
        print(f"  - averageRating: середній рейтинг (1-10)")
        print(f"  - numVotes: кількість голосів")
        print(f"• Загальна кількість записів: {total_lines:,}")
        if ratings:
            print(f"• Статистика рейтингів (перші 10000 записів):")
            print(f"  Мінімальний: {min(ratings):.1f}")
            print(f"  Максимальний: {max(ratings):.1f}")
            print(f"  Середній: {sum(ratings)/len(ratings):.2f}")
        print(f"• Можлива корисність: ДУЖЕ ВИСОКА (для аналізу популярності)")
except Exception as e:
    print(f"Помилка: {e}")
print()

# title.crew.tsv - Команда
print("📊 title.crew.tsv - РЕЖИСЕРИ ТА СЦЕНАРИСТИ")
print("-" * 60)
print(f"• Стовпці: tconst, directors, writers")
print(f"• Корисна інформація:")
print(f"  - tconst: ідентифікатор фільму")
print(f"  - directors: список режисерів (nconst)")
print(f"  - writers: список сценаристів (nconst)")
print(f"• Можлива корисність: висока (для аналізу впливу режисерів)")
print()

# title.principals.tsv - Головні учасники
print("📊 title.principals.tsv - ГОЛОВНІ УЧАСНИКИ")
print("-" * 60)
print(f"• Стовпці: tconst, ordering, nconst, category, job, characters")
print(f"• Корисна інформація:")
print(f"  - tconst: ідентифікатор фільму")
print(f"  - ordering: порядок в титрах")
print(f"  - nconst: ідентифікатор особи")
print(f"  - category: роль (actor, director, writer, etc.)")
print(f"  - characters: персонажі (для акторів)")
print(f"• Можлива корисність: висока (детальний акторський склад)")
print()

# title.akas.tsv - Альтернативні назви
print("📊 title.akas.tsv - АЛЬТЕРНАТИВНІ НАЗВИ")
print("-" * 60)
print(f"• Стовпці: titleId, ordering, title, region, language, types, attributes, isOriginalTitle")
print(f"• Корисна інформація:")
print(f"  - titleId: ідентифікатор фільму")
print(f"  - title: локалізована назва")
print(f"  - region: регіон/країна")
print(f"  - language: мова")
print(f"• Можлива корисність: НИЗЬКА (багато даних, але менш критична)")
print()

# title.episode.tsv - Епізоди
print("📊 title.episode.tsv - ЕПІЗОДИ СЕРІАЛІВ")
print("-" * 60)
print(f"• Стовпці: tconst, parentTconst, seasonNumber, episodeNumber")
print(f"• Корисна інформація:")
print(f"  - tconst: ідентифікатор епізоду")
print(f"  - parentTconst: ідентифікатор серіалу")
print(f"  - seasonNumber: номер сезону")
print(f"  - episodeNumber: номер епізоду")
print(f"• Можлива корисність: середня (залежить від фокусу аналізу)")
print()

print("\n3. ВИСНОВКИ ТА РЕКОМЕНДАЦІЇ:\n")
print("=" * 80)
print("\n✅ НАЙКОРИСНІШІ ФАЙЛИ (пріоритет для аналізу):")
print("   1. title.basics.tsv - основна інформація про фільми")
print("   2. title.ratings.tsv - рейтинги та популярність")
print("   3. name.basics.tsv - інформація про людей")
print("   4. title.crew.tsv - режисери та сценаристи")

print("\n⚠️  ДОДАТКОВІ ФАЙЛИ (за потреби):")
print("   5. title.principals.tsv - детальний акторський склад")
print("   6. title.episode.tsv - для аналізу серіалів")

print("\n❌ МЕНШ КОРИСНІ ФАЙЛИ:")
print("   7. title.akas.tsv - великий обсяг, але менш критична інформація")
print("      (локалізовані назви, можна використати для фільтрації по регіонах)")

print("\n📋 ТИП ДАНИХ:")
print("   • Структуровані табличні дані (TSV формат)")
print("   • Реляційна структура (з ідентифікаторами tconst, nconst)")
print("   • Містить текстові, числові та категоріальні змінні")
print("   • Дані IMDb (Internet Movie Database)")

print("\n🎯 МОЖЛИВІ НАПРЯМКИ АНАЛІЗУ:")
print("   • Аналіз трендів кінематографу за роками")
print("   • Дослідження популярних жанрів")
print("   • Вплив режисерів/акторів на рейтинги")
print("   • Аналіз тривалості фільмів")
print("   • Географічний розподіл виробництва")
print("   • Кореляція між роком випуску та рейтингом")

print("\n💡 РЕКОМЕНДАЦІЇ З ОБРОБКИ:")
print("   • Використовувати pandas для обробки")
print("   • Читати великі файли частинами (chunksize)")
print(r"   • Фільтрувати непотрібні дані (isAdult, \N значення)")
print("   • Об'єднувати таблиці через tconst/nconst")
print("   • Зосередитись на якісних даних (високий numVotes)")

print("\n" + "=" * 80)
print("АНАЛІЗ ЗАВЕРШЕНО")
print("=" * 80)
