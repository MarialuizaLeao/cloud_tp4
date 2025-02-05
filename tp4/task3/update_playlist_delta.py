from pyspark.sql import SparkSession
from delta.tables import DeltaTable
import time

def update_playlist_layer(spark, output_path):
    start_time = time.time()
    print(f"[INFO] Updating {output_path} Layer for playlist 11992...")
    
    playlist_path = f"{output_path}/playlists"
    
    if DeltaTable.isDeltaTable(spark, playlist_path):
        delta_table = DeltaTable.forPath(spark, playlist_path)
        delta_table.update(
            condition="playlist_id = 11992",
            set={
                "name": "'GYM WORKOUT'",
                "collaborative": "true"
            }
        )
    else:
        print(f"[ERROR] {output_path} Layer table not found.")
    
    end_time = time.time()
    print(f"[INFO] {output_path} Layer updated in {end_time - start_time:.2f} seconds.")

def main():
    start = time.time()
    
    spark = SparkSession.builder.appName("spotify-datalake-update") \
        .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.port.maxRetries", "50") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    
    silver_output_path = 'datalake/silver'
    gold_output_path = 'datalake/gold'
    
    update_playlist_layer(spark, silver_output_path)
    update_playlist_layer(spark, gold_output_path)
    
    end = time.time()
    print(f"[INFO] Playlist 11992 update completed in {end - start:.2f} seconds.")
    
    spark.stop()
    
if __name__ == "__main__":
    main()
