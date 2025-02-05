from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType
from pyspark.sql.functions import col, countDistinct, sum, when
import time
import sys
import pyspark

def write_bronze_layer(spark, bronze_playlists_path, bronze_tracks_path, bronze_output_path, format_="parquet"):
    playlists_v1_df = spark.read.json(bronze_playlists_path)
    tracks_v1_df = spark.read.json(bronze_tracks_path)
    
    playlists_v1_df.write.format(format_).mode("overwrite").save(f"{bronze_output_path}/playlists")
    tracks_v1_df.write.format(format_).mode("overwrite").save(f"{bronze_output_path}/tracks")
    return playlists_v1_df, tracks_v1_df

def write_silver_layer(spark, silver_output_path, playlists_v1_df, tracks_v1_df, format_="parquet"):
    start_time = time.time()
    
    song_silver_df = tracks_v1_df.select("track_name", "track_uri", "duration_ms", "album_uri", "artist_uri")
    song_silver_df.write.format(format_).mode("overwrite").save(f"{silver_output_path}/songs")
    
    album_silver_df = tracks_v1_df.select("album_uri", "album_name", "artist_uri").distinct()
    album_silver_df.write.format(format_).mode("overwrite").save(f"{silver_output_path}/albums")
    
    artist_silver_df = tracks_v1_df.select("artist_uri", "artist_name").distinct()
    artist_silver_df.write.format(format_).mode("overwrite").save(f"{silver_output_path}/artists")
    
    playlist_silver_df = playlists_v1_df.select(col("pid").alias("playlist_id"), "name", "collaborative", "description")
    playlist_silver_df.write.format(format_).mode("overwrite").save(f"{silver_output_path}/playlists")
    
    playlist_tracks_silver_df = tracks_v1_df.select(
        col("pid").alias("playlist_id"), "track_uri", "album_uri", "artist_uri", "pos", "duration_ms"
    )
    playlist_tracks_silver_df.write.format(format_).mode("overwrite").save(f"{silver_output_path}/playlist_tracks")
    
    end_time = time.time()
    return song_silver_df, album_silver_df, artist_silver_df, playlist_silver_df, playlist_tracks_silver_df, end_time - start_time

def write_gold_layer(spark, gold_output_path, song_silver_df, album_silver_df, artist_silver_df, playlist_silver_df, playlist_tracks_silver_df, format_="parquet"):
    start_time = time.time()
    
    playlist_gold_df = playlist_tracks_silver_df.groupBy("playlist_id").agg(
        countDistinct("track_uri").alias("num_tracks"),
        countDistinct("artist_uri").alias("num_artists"),
        countDistinct("album_uri").alias("num_albums"),
        sum("duration_ms").alias("total_duration_ms")
    ).join(playlist_silver_df, "playlist_id", "left")
    playlist_gold_df.write.format(format_).mode("overwrite").save(f"{gold_output_path}/playlists")
    
    playlist_tracks_gold_df = playlist_tracks_silver_df.alias("pt").join(
        song_silver_df.alias("s"), col("pt.track_uri") == col("s.track_uri"), "left"
    ).join(
        album_silver_df.alias("a"), col("pt.album_uri") == col("a.album_uri"), "left"
    ).join(
        artist_silver_df.alias("ar"), col("pt.artist_uri") == col("ar.artist_uri"), "left"
    ).select("pt.playlist_id", "pt.pos", "s.track_name", "a.album_name", "ar.artist_name")
    playlist_tracks_gold_df.write.format(format_).mode("overwrite").save(f"{gold_output_path}/playlist_tracks")
    
    end_time = time.time()
    return end_time - start_time

def main():
    if len(sys.argv) > 1:
        format_ = str(sys.argv[1]).lower()
    else:
        format_ = "parquet"

    print(format_)
    
    start = time.time()
    
    spark = SparkSession.builder.appName("spotify-datalake")
    
    if format_ == "delta":
        spark = spark.config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0") \
                     .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
                     .config("spark.port.maxRetries", "50") \
                     .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    
    spark = spark.getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    
    first_sample_playlists_path = '/shared/sampled/playlists_v1.json'
    first_sample_tracks_path = '/shared/sampled/tracks_v1.json'
    
    bronze_output_path = f'datalake/bronze'
    silver_output_path = f'datalake/silver'
    gold_output_path = f'datalake/gold'
    
    playlists_v1_df, tracks_v1_df = write_bronze_layer(spark, first_sample_playlists_path, first_sample_tracks_path, bronze_output_path, format_)
    song_silver_df, album_silver_df, artist_silver_df, playlist_silver_df, playlist_tracks_silver_df, time_for_update_silver_layer = write_silver_layer(spark, silver_output_path, playlists_v1_df, tracks_v1_df, format_)
    time_for_update_gold_layer = write_gold_layer(spark, gold_output_path, song_silver_df, album_silver_df, artist_silver_df, playlist_silver_df, playlist_tracks_silver_df, format_)
    
    end = time.time()
    
    print(f"Processing time for {format_} data lake: {end - start:.2f} seconds")
    
if __name__ == "__main__":
    main()
