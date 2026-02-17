"""
Перевірка коректності завантаження датасету IMDb
Автор: Яйко Назар
Етап: Підготовка
"""

import os
from pathlib import Path

# Очікувані файли та мінімальний розмір (в MB)
EXPECTED_FILES = {
    'name.basics.tsv': 600,        # ~700 MB після розпакування
    'title.basics.tsv': 700,       # ~800 MB
    'title.ratings.tsv': 20,       # ~25 MB
    'title.crew.tsv': 250,         # ~300 MB
    'title.principals.tsv': 1400,  # ~1.5 GB
    'title.akas.tsv': 1100,        # ~1.2 GB
    'title.episode.tsv': 180       # ~200 MB
}

def validate_dataset():
    """Перевірка наявності та розміру файлів датасету"""
    dataset_dir = Path('dataset')
    
    if not dataset_dir.exists():
        print("❌ Директорія 'dataset' не знайдена!")
        return False
    
    print("Перевірка датасету IMDb...\n")
    all_valid = True
    
    for filename, min_size_mb in EXPECTED_FILES.items():
        filepath = dataset_dir / filename
        
        if not filepath.exists():
            print(f"❌ {filename} - НЕ ЗНАЙДЕНО")
            all_valid = False
            continue
        
        # Перевірка розміру файлу
        size_mb = filepath.stat().st_size / (1024 * 1024)
        
        if size_mb < min_size_mb:
            print(f"⚠️  {filename} - {size_mb:.1f} MB (очікується >{min_size_mb} MB)")
            all_valid = False
        else:
            print(f"✓  {filename} - {size_mb:.1f} MB")
        
        # Перевірка перших рядків
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                first_line = f.readline()
                if '\t' not in first_line:
                    print(f"   ⚠️ Файл не має TAB розділювачів")
                    all_valid = False
        except Exception as e:
            print(f"   ❌ Помилка читання: {e}")
            all_valid = False
    
    print("\n" + "="*50)
    if all_valid:
        print("✓ Всі файли датасету валідні!")
    else:
        print("❌ Виявлено проблеми з датасетом")
    
    return all_valid

def check_line_counts():
    """Перевірка кількості рядків у файлах"""
    print("\nПеревірка кількості записів...")
    
    expected_counts = {
        'name.basics.tsv': 15_000_000,
        'title.basics.tsv': 12_000_000,
        'title.ratings.tsv': 1_600_000,
        'title.crew.tsv': 12_000_000,
    }
    
    for filename, min_count in expected_counts.items():
        filepath = Path('dataset') / filename
        
        if not filepath.exists():
            continue
            
        # Швидкий підрахунок рядків
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                count = sum(1 for _ in f) - 1  # -1 для header
            
            status = "✓" if count >= min_count else "⚠️"
            print(f"{status} {filename}: {count:,} записів")
        except Exception as e:
            print(f"❌ {filename}: Помилка - {e}")

if __name__ == "__main__":
    print("="*50)
    print("ПЕРЕВІРКА ДАТАСЕТУ IMDB")
    print("="*50 + "\n")
    
    if validate_dataset():
        check_line_counts()
    
    print("\n" + "="*50)
