from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType
from pyspark.sql.functions import col, countDistinct, sum, when
import time
import sys
from py4j.java_gateway import java_import

def get_parquet_storage(spark):
    # Import necessary Java classes for Hadoop FileSystem
    java_import(spark._jvm, "org.apache.hadoop.fs.FileSystem")
    java_import(spark._jvm, "org.apache.hadoop.fs.Path")

    # Get Hadoop FileSystem instance
    fs = spark._jvm.FileSystem.get(spark._jsc.hadoopConfiguration())

    # Define paths for all layers in Parquet-based Data Lake (Task 2)
    parquet_lake_paths = {
        "Bronze Layer": "datalake/bronze",
        "Silver Layer": "datalake/silver",
        "Gold Layer": "datalake/gold"
    }

    # Function to calculate storage size in MB
    def get_storage_size(path):
        try:
            return fs.getContentSummary(spark._jvm.Path(path)).getLength() / (1024 * 1024)  # Convert bytes to MB
        except:
            return 0  # If the path doesn't exist, return 0

    # Measure storage for Parquet-based Data Lake
    parquet_storage_results = {}
    parquet_total_storage = 0

    for layer, path in parquet_lake_paths.items():
        size_mb = get_storage_size(path)
        parquet_storage_results[layer] = round(size_mb, 2)
        parquet_total_storage += size_mb

    # Add total storage
    parquet_storage_results["Total Storage"] = round(parquet_total_storage, 2)

    print(f"Bronze layer storage: {parquet_storage_results['Bronze Layer']} mb")
    print(f"Silver layer storage: {parquet_storage_results['Silver Layer']} mb")
    print(f"Gold layer storage: {parquet_storage_results['Gold Layer']} mb")
    print(f"Total storage: {parquet_storage_results['Total Storage']} mb")
    
def update_silver_layer(spark, silver_output_path, playlist_v2_df, tracks_v2_df):
    start_time = time.time()
    existing_playlists_df = spark.read.parquet(f"{silver_output_path}/playlists")
    existing_songs_df = spark.read.parquet(f"{silver_output_path}/songs")
    existing_albums_df = spark.read.parquet(f"{silver_output_path}/albums")
    existing_artists_df = spark.read.parquet(f"{silver_output_path}/artists")
    existing_playlist_tracks_df = spark.read.parquet(f"{silver_output_path}/playlist_tracks")
    # Update Songs
    start_time_songs = time.time()
    new_song_silver_df = tracks_v2_df.select(
        col("track_name"),
        col("track_uri"),
        col("duration_ms"),
        col("album_uri"),
        col("artist_uri")
    ).distinct()
    existing_songs_df = existing_songs_df.unionByName(
        new_song_silver_df, 
        allowMissingColumns=True
    ).dropDuplicates(["track_uri"])
    end_time_songs = time.time() 

    # Update Albums
    start_time_albums = time.time()
    new_album_silver_df = tracks_v2_df.select(
        col("album_uri"),
        col("album_name"),
        col("artist_uri")
    ).distinct()
    existing_albums_df = existing_albums_df.unionByName(
        new_album_silver_df,
        allowMissingColumns=True
    ).dropDuplicates(["album_uri"])
    end_time_albums = time.time()

    # Update Artists
    start_time_artists = time.time()
    new_artist_silver_df = tracks_v2_df.select(
        col("artist_uri"),
        col("artist_name")
    ).distinct()
    existing_artists_df = existing_artists_df.unionByName(
        new_artist_silver_df, 
        allowMissingColumns=True
    ).dropDuplicates(["artist_uri"])
    end_time_artists = time.time()

    # Update Playlists
    start_time_playlists = time.time()
    new_playlist_silver_df = playlist_v2_df.select(
        col("pid").alias("playlist_id"),
        col("name"),
        col("collaborative"),
        col("description")
    )
    existing_playlists_df = existing_playlists_df.unionByName(
        new_playlist_silver_df, 
        allowMissingColumns=True
    ).dropDuplicates(["playlist_id"])
    existing_playlists_df = existing_playlists_df.alias("old").join(
        new_playlist_silver_df.alias("new"), "playlist_id", "left"
    ).select(
        col("old.playlist_id").alias("playlist_id"),
        when(col("new.name").isNotNull(), col("new.name")).otherwise(col("old.name")).alias("name"),
        when(col("new.description").isNotNull(), col("new.description")).otherwise(col("old.description")).alias("description"),
        when(col("new.collaborative").isNotNull(), col("new.collaborative")).otherwise(col("old.collaborative")).alias("collaborative")
    ).dropDuplicates(["playlist_id"])
    end_time_playlists = time.time()

    # Update Playlist Tracks
    start_time_tracks = time.time()
    new_playlist_tracks_silver_df = tracks_v2_df.select(
        col("pid").alias("playlist_id"),
        col("track_uri"),
        col("album_uri"),
        col("artist_uri"),
        col("pos"),
        col("duration_ms")
    )
    existing_playlist_tracks_df = existing_playlist_tracks_df.alias("old").join(
        new_playlist_tracks_silver_df.alias("new"), (col("old.playlist_id") == col("new.playlist_id")) & (col("old.pos") == col("new.pos")), "outer"
    ).select(
        when(col("new.playlist_id").isNotNull(), col("new.playlist_id")).otherwise(col("old.playlist_id")).alias("playlist_id"),
        when(col("new.pos").isNotNull(), col("new.pos")).otherwise(col("old.pos")).alias("pos"),
        when(col("new.track_uri").isNotNull(), col("new.track_uri")).otherwise(col("old.track_uri")).alias("track_uri"),
        when(col("new.album_uri").isNotNull(), col("new.album_uri")).otherwise(col("old.album_uri")).alias("album_uri"),
        when(col("new.artist_uri").isNotNull(), col("new.artist_uri")).otherwise(col("old.artist_uri")).alias("artist_uri"),
        when(col("new.duration_ms").isNotNull(), col("new.duration_ms")).otherwise(col("old.duration_ms")).alias("duration_ms")
    ).dropDuplicates(["playlist_id", "pos"])
    end_time_tracks = time.time()
    # Write new versions
    existing_songs_df.write.format("parquet").mode("overwrite").save(f"{silver_output_path}/songs")
    existing_albums_df.write.format("parquet").mode("overwrite").save(f"{silver_output_path}/albums")
    existing_artists_df.write.format("parquet").mode("overwrite").save(f"{silver_output_path}/artists")
    existing_playlists_df.write.format("parquet").mode("overwrite").save(f"{silver_output_path}/playlists")
    existing_playlist_tracks_df.write.format("parquet").mode("overwrite").save(f"{silver_output_path}/playlist_tracks")
    
    end_time = time.time()
    
    return end_time - start_time, end_time_songs - start_time_songs , end_time_albums - start_time_albums, end_time_artists - start_time_artists , end_time_playlists - start_time_playlists , end_time_tracks - start_time_tracks

def update_gold_layer(spark, silver_output_path, gold_output_path):
    start_time = time.time()
    existing_playlists_df = spark.read.parquet(f"{silver_output_path}/playlists")
    existing_songs_df = spark.read.parquet(f"{silver_output_path}/songs")
    existing_albums_df = spark.read.parquet(f"{silver_output_path}/albums")
    existing_artists_df = spark.read.parquet(f"{silver_output_path}/artists")
    existing_playlist_tracks_df = spark.read.parquet(f"{silver_output_path}/playlist_tracks")

    start_time_playlist = time.time()
    gold_playlist_df = existing_playlist_tracks_df.groupBy("playlist_id").agg(
        countDistinct("track_uri").alias("num_tracks"),
        countDistinct("artist_uri").alias("num_artists"),
        countDistinct("album_uri").alias("num_albums"),
        sum("duration_ms").alias("total_duration_ms")
    ).join(existing_playlists_df, "playlist_id", "inner")
    end_time_playlist = time.time()

    start_time_tracks = time.time()
    gold_playlist_tracks_df = existing_playlist_tracks_df.join(
        existing_songs_df, "track_uri", "inner"
    ).join(
        existing_albums_df, "album_uri", "inner"
    ).join(
        existing_artists_df, "artist_uri", "inner"
    ).select(
        col("playlist_id"),
        col("pos"),
        col("track_name"),
        col("album_name"),
        col("artist_name")
    )
    end_time_tracks = time.time()

    gold_playlist_df.write.format("parquet").mode("overwrite").save(f"{gold_output_path}/playlists")
    gold_playlist_tracks_df.write.format("parquet").mode("overwrite").save(f"{gold_output_path}/playlist_tracks")
    end_time = time.time()
    return end_time - start_time, end_time_playlist - start_time_playlist , end_time_tracks - start_time_tracks
    
def main():
    if len(sys.argv) > 1:
        sample_playlist_v2 = str(sys.argv[2])
        sample_tracks_v2 = str(sys.argv[4])
    else:
        sample_playlist_v2 = '/shared/sampled/playlists_v2.json'
        sample_tracks_v2 = '/shared/sampled/tracks_v2.json'
    
    start = time.time()
    
    print(sample_playlist_v2, sample_tracks_v2)

    # Initialize Spark Session with Delta Lake support
    spark = SparkSession.builder \
        .appName("spotify-datalake") \
        .config("spark.jars.packages", "io.delta:delta-core_2.13:2.0.0") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.executor.instances", "2") \
        .config("spark.executor.cores", "2") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "4g") \
        .config("spark.ui.port", "4060") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    sc = spark.sparkContext

    # Data Lake paths
    bronze_output_path = 'datalake/bronze'
    silver_output_path = 'datalake/silver'
    gold_output_path = 'datalake/gold'
    
    # Load data into DataFrames
    playlist_v1_df = spark.read.parquet(f"{bronze_output_path}/playlists")
    tracks_v1_df = spark.read.parquet(f"{bronze_output_path}/tracks")
    playlist_v2_df = spark.read.json(sample_playlist_v2)
    tracks_v2_df = spark.read.json(sample_tracks_v2)
    
    time_for_update_silver_layer, silver_songs, silver_albums, silver_artists, silver_playlists, silver_tracks = update_silver_layer(spark, silver_output_path, playlist_v2_df, tracks_v2_df)
    time_for_update_gold_layer, gold_playlists, gold_tracks = update_gold_layer(spark, silver_output_path, gold_output_path)
    
    end = time.time()
    
    title = ["Total", "Gold layer", "Silver layer"]
    times = [end-start, time_for_update_gold_layer, time_for_update_silver_layer]
    
    print(f"------------------------")
    for i in range(3):
        print(f"{title[i]}: {times[i]:.3f}")
        print(f"------------------------")

    get_parquet_storage(spark)
    
    # Stop Spark Session
    spark.stop()

if __name__ == "__main__":
    main()