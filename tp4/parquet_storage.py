from py4j.java_gateway import java_import

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
