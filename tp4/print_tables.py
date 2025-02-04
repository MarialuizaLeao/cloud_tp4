from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("delta-lake-inspect") \
    .config("spark.jars.packages", "io.delta:delta-core_2.13:2.4.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

silver_path = "datalake/silver"
gold_path = "datalake/gold"

def show_first_10_rows(table_path):
    try:
        df = spark.read.format("parquet").load(table_path)
        df.show(13, truncate=False)
    except Exception as e:
        print(f"Erro ao ler a tabela em {table_path}: {e}")


tables = [
    f"{silver_path}/songs",
    f"{silver_path}/albums",
    f"{silver_path}/artists",
    f"{silver_path}/playlists",
    f"{silver_path}/playlist_tracks",
    f"{gold_path}/playlist_tracks",
    f"{gold_path}/playlists"
]

for table in tables:
    print(f"Primeiras 10 linhas de {table}:")
    show_first_10_rows(table)
    print("-" * 50)

spark.stop()
