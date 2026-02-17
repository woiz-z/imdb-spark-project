"""
Автоматична перевірка сумісності версій
Автор: Чігур Володимир
Етап: Підготовка
"""

def check_compatibility():
    """Перевірка всіх залежностей та їх сумісності"""
    
    print("="*60)
    print("ПЕРЕВІРКА СУМІСНОСТІ ВЕРСІЙ")
    print("="*60 + "\n")
    
    errors = []
    warnings = []
    
    # 1. Python
    import sys
    python_version = f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}"
    print(f"Python: {python_version}")
    
    if sys.version_info < (3, 8):
        errors.append("Python версія < 3.8")
    elif sys.version_info >= (3, 12):
        warnings.append("Python версія >= 3.12 (не тестована з PySpark 3.5)")
    else:
        print("  ✓ Версія підтримується")
    
    # 2. PySpark
    try:
        import pyspark
        pyspark_version = pyspark.__version__
        print(f"\nPySpark: {pyspark_version}")
        
        if pyspark_version < "3.5.0":
            errors.append("PySpark версія < 3.5.0")
        else:
            print("  ✓ Версія підтримується")
    except ImportError:
        errors.append("PySpark не встановлено")
        print("\n❌ PySpark не встановлено")
    
    # 3. Pandas
    try:
        import pandas as pd
        pandas_version = pd.__version__
        print(f"\nPandas: {pandas_version}")
        
        if pd.__version__ < "2.2.0":
            warnings.append("Pandas версія < 2.2.0 (рекомендується оновлення)")
        else:
            print("  ✓ Версія підтримується")
    except ImportError:
        errors.append("Pandas не встановлено")
        print("\n❌ Pandas не встановлено")
    
    # 4. NumPy
    try:
        import numpy as np
        numpy_version = np.__version__
        print(f"\nNumPy: {numpy_version}")
        
        if np.__version__.startswith('2.'):
            errors.append("NumPy версія 2.x не сумісна з PySpark 3.5")
        elif np.__version__ < "1.26.0":
            warnings.append("NumPy версія < 1.26.0")
        else:
            print("  ✓ Версія підтримується")
    except ImportError:
        errors.append("NumPy не встановлено")
        print("\n❌ NumPy не встановлено")
    
    # 5. PyArrow
    try:
        import pyarrow as pa
        pyarrow_version = pa.__version__
        print(f"\nPyArrow: {pyarrow_version}")
        
        if pa.__version__ < "15.0.0":
            warnings.append("PyArrow версія < 15.0.0")
        else:
            print("  ✓ Версія підтримується")
    except ImportError:
        warnings.append("PyArrow не встановлено (опціонально, але рекомендується)")
        print("\n⚠️  PyArrow не встановлено (опціонально)")
    
    # 6. Java (через PySpark)
    if 'pyspark' in sys.modules:
        try:
            from pyspark.sql import SparkSession
            spark = SparkSession.builder \
                .appName("Version Check") \
                .master("local[1]") \
                .getOrCreate()
            
            java_version = spark.sparkContext._gateway.jvm.System.getProperty("java.version")
            print(f"\nJava: {java_version}")
            
            if java_version.startswith("1.8") or java_version.startswith("11") or java_version.startswith("17"):
                print("  ✓ Версія підтримується")
            else:
                warnings.append(f"Java версія {java_version} не тестована")
            
            spark.stop()
        except Exception as e:
            errors.append(f"Не вдалося перевірити Java: {e}")
            print(f"\n❌ Помилка перевірки Java: {e}")
    
    # Результат
    print("\n" + "="*60)
    
    if errors:
        print("❌ КРИТИЧНІ ПОМИЛКИ:")
        for error in errors:
            print(f"  - {error}")
    
    if warnings:
        print("\n⚠️  ПОПЕРЕДЖЕННЯ:")
        for warning in warnings:
            print(f"  - {warning}")
    
    if not errors and not warnings:
        print("✓ ВСІ ВЕРСІЇ СУМІСНІ!")
    elif not errors:
        print("\n✓ Немає критичних помилок, але є попередження")
    else:
        print("\n❌ Виявлено критичні помилки сумісності")
    
    print("="*60)
    
    return len(errors) == 0

if __name__ == "__main__":
    success = check_compatibility()
    
    if not success:
        print("\nВстановіть відсутні пакети командою:")
        print("pip install -r requirements.txt")
    
    import sys
    sys.exit(0 if success else 1)
