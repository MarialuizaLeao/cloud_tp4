from pyspark.sql import SparkSession
from pyspark.sql.functions import col, countDistinct, sum
from delta.tables import DeltaTable
import time
import sys
from py4j.java_gateway import java_import
from pyspark.sql import functions as F

def get_delta_storage(spark):
    java_import(spark._jvm, "org.apache.hadoop.fs.FileSystem")
    java_import(spark._jvm, "org.apache.hadoop.fs.Path")
    
    fs = spark._jvm.FileSystem.get(spark._jsc.hadoopConfiguration())
    
    delta_lake_paths = {
        "Bronze Layer": "datalake/bronze",
        "Silver Layer": "datalake/silver",
        "Gold Layer": "datalake/gold"
    }
    
    def get_storage_size(path):
        try:
            return fs.getContentSummary(spark._jvm.Path(path)).getLength() / (1024 * 1024)
        except: 
            return 0
    
    delta_storage_results = {}
    delta_total_storage = 0
    
    for layer, path in delta_lake_paths.items():
        size_mb = get_storage_size(path)
        delta_storage_results[layer] = round(size_mb, 2)
        delta_total_storage += size_mb
    
    delta_storage_results["Total Storage"] = round(delta_total_storage, 2)
    
    print(f"Bronze layer storage: {delta_storage_results['Bronze Layer']} mb")
    print(f"Silver layer storage: {delta_storage_results['Silver Layer']} mb")
    print(f"Gold layer storage: {delta_storage_results['Gold Layer']} mb")
    print(f"Total storage: {delta_storage_results['Total Storage']} mb")

def merge_delta_table(spark, new_data, target_path, join_condition, update_columns):
    delta_table = DeltaTable.forPath(spark, target_path)
    
    delta_table.alias("target").merge(
        new_data.alias("source"), join_condition
    ).whenMatchedUpdate(
        set={col: F.col("source." + col) for col in update_columns}
    ).whenNotMatchedInsert(
        values={col: F.col("source." + col) for col in update_columns}
    ).execute()

def update_silver_layer(spark, silver_output_path, playlist_v2_df, tracks_v2_df):
    start_time = time.time()

    new_playlist_silver_df = playlist_v2_df.select(col("pid").alias("playlist_id"), "name", "collaborative", "description").dropDuplicates(["playlist_id"])
    new_playlist_tracks_silver_df = tracks_v2_df.select(col("pid").alias("playlist_id"), "track_uri", "album_uri", "artist_uri", "pos", "duration_ms").dropDuplicates(["playlist_id", "track_uri"])
    
    merge_delta_table(spark, new_playlist_silver_df, f"{silver_output_path}/playlists", "target.playlist_id = source.playlist_id", ["playlist_id", "name", "collaborative", "description"])
    merge_delta_table(spark, new_playlist_tracks_silver_df, f"{silver_output_path}/playlist_tracks", "target.playlist_id = source.playlist_id AND target.track_uri = source.track_uri", ["playlist_id", "track_uri", "album_uri", "artist_uri", "pos", "duration_ms"])

    return time.time() - start_time


def update_gold_layer(spark, silver_output_path, gold_output_path):
    start_time = time.time()

    existing_playlist_tracks_df = spark.read.format("delta").load(f"{silver_output_path}/playlist_tracks")
    existing_playlists_df = spark.read.format("delta").load(f"{silver_output_path}/playlists")

    # Aggregate to ensure unique records
    gold_playlist_df = existing_playlist_tracks_df.groupBy("playlist_id").agg(
        countDistinct("track_uri").alias("num_tracks"),
        sum("duration_ms").alias("total_duration_ms")
    ).join(existing_playlists_df, "playlist_id", "inner").dropDuplicates(["playlist_id"])

    # MERGE instead of overwrite
    merge_delta_table(
        spark, gold_playlist_df, f"{gold_output_path}/playlists",
        "target.playlist_id = source.playlist_id",
        ["playlist_id", "total_duration_ms", "name", "collaborative", "description"]
    )

    return time.time() - start_time


def main():
    if len(sys.argv) > 1:
        sample_playlist_v2 = str(sys.argv[2])
        sample_tracks_v2 = str(sys.argv[4])
    else:
        sample_playlist_v2 = '/shared/sampled/playlists_v2.json'
        sample_tracks_v2 = '/shared/sampled/tracks_v2.json'
    
    spark = SparkSession.builder.appName("spotify-datalake") \
        .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.port.maxRetries", "50") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.executor.instances", "2") \
        .config("spark.executor.cores", "2") \
        .config("spark.executor.memory", "1024M") \
        .getOrCreate()
    
    silver_output_path = 'datalake/silver'
    gold_output_path = 'datalake/gold'
    
    playlist_v2_df = spark.read.json(sample_playlist_v2)
    tracks_v2_df = spark.read.json(sample_tracks_v2)
    
    time_for_update_silver_layer = update_silver_layer(spark, silver_output_path, playlist_v2_df, tracks_v2_df)
    time_for_update_gold_layer = update_gold_layer(spark, silver_output_path, gold_output_path)
    
    print(f"Silver Layer Update Time: {time_for_update_silver_layer:.3f} seconds")
    print(f"Gold Layer Update Time: {time_for_update_gold_layer:.3f} seconds")

    get_delta_storage(spark)
    
    spark.stop()
    
if __name__ == "__main__":
    main()