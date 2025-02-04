from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType
from pyspark.sql.functions import col, countDistinct, sum, when
import time
import sys

sample_playlist_v1 = '/shared/sampled/playlists_v1.json'
sample_tracks_v1 = '/shared/sampled/tracks_v1.json'
sample_playlist_v2 = '/shared/sampled/playlists_v2.json'
sample_tracks_v2 = '/shared/sampled/tracks_v2.json'

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
playlist_v1_df = spark.read.json(sample_playlist_v1)
tracks_v1_df = spark.read.json(sample_tracks_v1)
playlist_v2_df = spark.read.json(sample_playlist_v2)
tracks_v2_df = spark.read.json(sample_tracks_v2)

existing_playlists_df = spark.read.parquet(f"{silver_output_path}/playlists")
existing_songs_df = spark.read.parquet(f"{silver_output_path}/songs")
existing_albums_df = spark.read.parquet(f"{silver_output_path}/albums")
existing_artists_df = spark.read.parquet(f"{silver_output_path}/artists")
existing_playlist_tracks_df = spark.read.parquet(f"{silver_output_path}/playlist_tracks")

# Update Songs, Albums, Artists, and Playlist Tracks
new_song_silver_df = tracks_v2_df.select(
    col("track_name"),
    col("track_uri"),
    col("duration_ms"),
    col("album_uri"),
    col("artist_uri")
)
existing_songs_df = existing_songs_df.unionByName(
    new_song_silver_df,
    allowMissingColumns=True
).dropDuplicates(["track_uri"])

new_album_silver_df = tracks_v2_df.select(
    col("album_uri"),
    col("album_name"),
    col("artist_uri")
).distinct()

existing_albums_df = existing_albums_df.unionByName(
    new_album_silver_df,
    allowMissingColumns=True
).dropDuplicates(["album_uri"])

new_artist_silver_df = tracks_v2_df.select(
    col("artist_uri"),
    col("artist_name")
).distinct()

existing_artists_df = existing_artists_df.unionByName(
    new_artist_silver_df,
    allowMissingColumns=True
).dropDuplicates(["artist_uri"])

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

# Update Tracks - Replace songs if pos is occupied
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

existing_songs_df.write.format("parquet").mode("overwrite").save(f"{silver_output_path}/songs")
existing_albums_df.write.format("parquet").mode("overwrite").save(f"{silver_output_path}/albums")
existing_artists_df.write.format("parquet").mode("overwrite").save(f"{silver_output_path}/artists")
existing_playlists_df.write.format("parquet").mode("overwrite").save(f"{silver_output_path}/playlists")
existing_playlist_tracks_df.write.format("parquet").mode("overwrite").save(f"{silver_output_path}/playlist_tracks")

gold_playlist_df = existing_playlist_tracks_df.groupBy("playlist_id").agg(
    countDistinct("track_uri").alias("num_tracks"),
    countDistinct("artist_uri").alias("num_artists"),
    countDistinct("album_uri").alias("num_albums"),
    sum("duration_ms").alias("total_duration_ms")
).join(existing_playlists_df, "playlist_id", "inner")

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

gold_playlist_df.write.format("parquet").mode("overwrite").save(f"{gold_output_path}/playlists")
gold_playlist_tracks_df.write.format("parquet").mode("overwrite").save(f"{gold_output_path}/playlist_tracks")

# Stop Spark Session
spark.stop()