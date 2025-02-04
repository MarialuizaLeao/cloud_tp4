from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType
from pyspark.sql.functions import col, countDistinct, sum, when
import time
import sys
import pyspark

def write_bronze_layer(spark, bronze_playlists_path, bronze_tracks_path, bronze_output_path, format_ = "parquet"):
    # Load data into DataFrames
    playlists_v1_df = spark.read.json(bronze_playlists_path)
    tracks_v1_df = spark.read.json(bronze_tracks_path)

    # Write to Bronze Layer
    playlists_v1_df.write.format(format_).mode("overwrite").save(f"{bronze_output_path}/playlists")
    tracks_v1_df.write.format(format_).mode("overwrite").save(f"{bronze_output_path}/tracks")
    return playlists_v1_df, tracks_v1_df

def write_silver_layer(spark, silver_output_path, playlists_v1_df, tracks_v1_df, format_ = "parquet"):
    start_time = time.time()
    start_time_songs = time.time()
    song_silver_df = tracks_v1_df.select(
        col("track_name"),
        col("track_uri"),
        col("duration_ms"),
        col("album_uri"),
        col("artist_uri")
    )
    song_silver_df.write.format(format_).mode("overwrite").save(f"{silver_output_path}/songs")
    end_time_songs = time.time() 
    # Album Information Table
    start_time_albums = time.time()
    album_silver_df = tracks_v1_df.select(
        col("album_uri").alias("album_uri"),
        col("album_name"),
        col("artist_uri")
    ).distinct()
    album_silver_df.write.format(format_).mode("overwrite").save(f"{silver_output_path}/albums")
    end_time_albums = time.time()
    # Artist Information Table
    start_time_artists = time.time()
    artist_silver_df = tracks_v1_df.select(
        col("artist_uri").alias("artist_uri"),
        col("artist_name")
    ).distinct()
    artist_silver_df.write.format(format_).mode("overwrite").save(f"{silver_output_path}/artists")
    end_time_artists = time.time()
    # Playlist Information Table
    start_time_playlists = time.time()
    playlist_silver_df = playlists_v1_df.select(
        col("pid").alias("playlist_id"),
        col("name"),
        col("collaborative"),
        col("description")
    )
    playlist_silver_df.write.format(format_).mode("overwrite").save(f"{silver_output_path}/playlists")
    end_time_playlists = time.time()
    
    start_time_tracks = time.time()
    playlist_tracks_silver_df = tracks_v1_df.select(
        col("pid").alias("playlist_id"),
        col("track_uri"),
        col("album_uri"),
        col("artist_uri"),
        col("pos"),
        col("duration_ms")
    )
    playlist_tracks_silver_df.write.format(format_).mode("overwrite").save(f"{silver_output_path}/playlist_tracks")
    end_time_tracks = time.time()
    end_time = time.time()
    return song_silver_df, album_silver_df, artist_silver_df, playlist_silver_df, playlist_tracks_silver_df, end_time - start_time, end_time_songs - start_time_songs , end_time_albums - start_time_albums, end_time_artists - start_time_artists , end_time_playlists - start_time_playlists , end_time_tracks - start_time_tracks

def write_gold_layer(spark, gold_output_path, song_silver_df, album_silver_df, artist_silver_df, playlist_silver_df, playlist_tracks_silver_df, format_ = "parquet"):
    start_time = time.time()
    # Playlist Information Aggregated Table
    start_time_playlist = time.time()
    playlist_gold_df = playlist_tracks_silver_df.groupBy("playlist_id").agg(
        countDistinct("track_uri").alias("num_tracks"),
        countDistinct("artist_uri").alias("num_artists"),
        countDistinct("album_uri").alias("num_albums"),
        sum("duration_ms").alias("total_duration_ms")
    ).join(playlist_silver_df, "playlist_id", "left")
    playlist_gold_df.write.format(format_).mode("overwrite").save(f"{gold_output_path}/playlists")
    end_time_playlist = time.time()
    # Playlist Tracks Aggregated Table
    start_time_tracks = time.time()
    playlist_tracks_gold_df = playlist_tracks_silver_df.alias("pt").join(
        song_silver_df.alias("s"), col("pt.track_uri") == col("s.track_uri"), "left"
    ).join(
        album_silver_df.alias("a"), col("pt.album_uri") == col("a.album_uri"), "left"
    ).join(
        artist_silver_df.alias("ar"), col("pt.artist_uri") == col("ar.artist_uri"), "left"
    ).select(
        col("pt.playlist_id"),
        col("pt.pos"),
        col("s.track_name"),
        col("a.album_name"),
        col("ar.artist_name")
    )
    playlist_tracks_gold_df.write.format(format_).mode("overwrite").save(f"{gold_output_path}/playlist_tracks")
    end_time_tracks = time.time()
    end_time = time.time()
    return end_time - start_time, end_time_playlist - start_time_playlist , end_time_tracks - start_time_tracks

def main():
    
    if len(sys.argv) > 1:
        format_ = str(sys.argv[2])
    else:
        format_ = "parquet"
    
    start = time.time()

    spark = SparkSession.builder \
        .appName("spotify-datalake") \
        .config("spark.jars.packages", "io.delta:delta-core_2.13:2.0.0") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.executor.instances", "2") \
        .config("spark.executor.cores", "2") \
        .config("spark.executor.memory", "1024M") \
        .config("spark.port.maxRetries", "50") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    sc = spark.sparkContext

    # Define file paths
    first_sample_playlists_path = '/shared/sampled/playlists_v1.json'
    first_sample_tracks_path = '/shared/sampled/tracks_v1.json'
    
    bronze_output_path = 'datalake/bronze'
    silver_output_path = 'datalake/silver'
    gold_output_path = 'datalake/gold'
    
    playlists_v1_df, tracks_v1_df = write_bronze_layer(spark, first_sample_playlists_path, first_sample_tracks_path, bronze_output_path, format_)
    song_silver_df, album_silver_df, artist_silver_df, playlist_silver_df, playlist_tracks_silver_df, time_for_update_silver_layer, silver_songs, silver_albums, silver_artists, silver_playlists, silver_tracks = write_silver_layer(spark, silver_output_path, playlists_v1_df, tracks_v1_df, format_)
    time_for_update_gold_layer, gold_playlists, gold_tracks = write_gold_layer(spark, gold_output_path, song_silver_df, album_silver_df, artist_silver_df, playlist_silver_df, playlist_tracks_silver_df, format_)
    end = time.time()
    title = ["Silver layer: Songs", "Silver layer: Albums", "Silver layer: Artists", "Silver layer: Playlists", "Silver layer: Playlists Tracks","Gold layer: Playlists", "Gold layer: Playlists Tracks"]
    times = [silver_songs, silver_albums, silver_artists, silver_playlists, silver_tracks, time_for_update_gold_layer, time_for_update_silver_layer, gold_playlists, gold_tracks]
    
    print(f"------------------------")
    for i in range(len(title)):
        print(f"{title[i]}: {times[i]:.4f}")
        print(f"------------------------")
    
    spark.stop()
    
if __name__ == "__main__":
    main()

