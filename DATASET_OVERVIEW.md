# Завантаження датасету та огляд структури даних

**Автор:** Іванчук Орест  
**Етап:** Підготовка  
**Дата:** Лютий 2026

## Створення гілок Git

### Базові гілки

```bash
# Створення гілки master (основна)
git checkout -b main

# Створення гілки develop (розробка)
git checkout -b develop
```

**Пояснення структури гілок:**
- `main` - стабільна версія проєкту
- `develop` - поточна розробка
- Feature гілки - для окремих завдань кожного учасника

## Завантаження датасету IMDb

### Джерело даних

IMDb Non-Commercial Datasets:  
https://datasets.imdbws.com/

### Завантажені файли (загальний розмір: 9+ GB)

| Файл | Розмір | Записів | Опис |
|------|--------|---------|------|
| `name.basics.tsv.gz` | ~700 MB | 15M+ | Інформація про людей |
| `title.basics.tsv.gz` | ~800 MB | 12M+ | Основна інформація про фільми |
| `title.ratings.tsv.gz` | ~25 MB | 1.6M+ | Рейтинги |
| `title.crew.tsv.gz` | ~300 MB | 12M+ | Команда (режисери, автори) |
| `title.principals.tsv.gz` | ~1.5 GB | 60M+ | Головні учасники |
| `title.akas.tsv.gz` | ~1.2 GB | 40M+ | Альтернативні назви |
| `title.episode.tsv.gz` | ~200 MB | 8M+ | Інформація про епізоди |

### Команди завантаження

```bash
cd E:\BBD\dataset

# Завантаження всіх файлів
curl -O https://datasets.imdbws.com/name.basics.tsv.gz
curl -O https://datasets.imdbws.com/title.basics.tsv.gz
curl -O https://datasets.imdbws.com/title.ratings.tsv.gz
curl -O https://datasets.imdbws.com/title.crew.tsv.gz
curl -O https://datasets.imdbws.com/title.principals.tsv.gz
curl -O https://datasets.imdbws.com/title.akas.tsv.gz
curl -O https://datasets.imdbws.com/title.episode.tsv.gz

# Розпакування
gunzip *.gz
```

## Початковий огляд структури даних

### 1. title.basics.tsv

**Поля:**
- `tconst` - унікальний ідентифікатор (string)
- `titleType` - тип (movie, tvSeries, short...)
- `primaryTitle` - основна назва
- `originalTitle` - оригінальна назва
- `isAdult` - дорослий контент (0/1)
- `startYear` - рік випуску
- `endYear` - рік завершення (для серіалів)
- `runtimeMinutes` - тривалість
- `genres` - жанри (comma-separated)

**Приклад запису:**
```
tt0000001   short   Carmencita    Carmencita    0   1894    \N    1    Documentary,Short
```

### 2. title.ratings.tsv

**Поля:**
- `tconst` - ідентифікатор фільму
- `averageRating` - середній рейтинг (1.0-10.0)
- `numVotes` - кількість голосів

**Приклад:**
```
tt0000001   5.7    2087
```

### 3. name.basics.tsv

**Поля:**
- `nconst` - ідентифікатор персони
- `primaryName` - ім'я
- `birthYear` - рік народження
- `deathYear` - рік смерті
- `primaryProfession` - професії
- `knownForTitles` - відомі роботи

### Особливості формату

- Розділювач: TAB (`\t`)
- NULL значення: `\N`
- Кодування: UTF-8
- Масиви: розділені комами

## Структура проєкту

```
BBD/
├── dataset/           # Датасети IMDb (9+ GB)
│   ├── name.basics.tsv
│   ├── title.basics.tsv
│   ├── title.ratings.tsv
│   ├── title.crew.tsv
│   ├── title.principals.tsv
│   ├── title.akas.tsv
│   └── title.episode.tsv
├── .venv/            # Віртуальне середовище
├── main.py           # Головний файл (буде створено)
├── requirements.txt  # Залежності
└── README.md         # Документація
```

## Висновки

✓ Гілки master та develop створено  
✓ Датасет IMDb завантажено (9+ GB, 7 файлів)  
✓ Структура даних проаналізована  
✓ Виявлено особливості формату (TAB, \N для NULL)  

**Статус:** Завершено
