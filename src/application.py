"""
NYC Taxi Tip Analysis - Big Data Project
Implements two pipelines: non-optimized (groupByKey) and optimized (broadcast + reduceByKey)
"""
import argparse
import json
import time
import os
from datetime import datetime

from pyspark.sql import SparkSession


def create_spark_session(app_name="NYC Taxi Analysis"):
    """Create Spark session with S3A configuration for LocalStack if needed"""
    builder = SparkSession.builder.appName(app_name)
    
    # Check if we're using LocalStack (endpoint set)
    s3_endpoint = os.environ.get("FS_S3A_ENDPOINT")
    if s3_endpoint:
        print(f"Configuring Spark for LocalStack: {s3_endpoint}")
        builder = builder \
            .config("spark.hadoop.fs.s3a.endpoint", s3_endpoint) \
            .config("spark.hadoop.fs.s3a.access.key", os.environ.get("AWS_ACCESS_KEY_ID", "test")) \
            .config("spark.hadoop.fs.s3a.secret.key", os.environ.get("AWS_SECRET_ACCESS_KEY", "test")) \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    
    return builder.getOrCreate()


def safe_hour(value):
    """Extract hour from datetime."""
    if value is None:
        return -1
    try:
        if hasattr(value, 'hour'):
            return int(value.hour)
        if isinstance(value, str):
            return datetime.strptime(value, "%Y-%m-%d %H:%M:%S").hour
        return -1
    except:
        return -1


def expand_path(path):
    """Expand wildcards in local paths"""
    import glob
    import os

    # If it's S3, return as-is (S3 handles wildcards natively)
    if path.startswith('s3://') or path.startswith('s3a://'):
        return path

    # If it's a directory, return as-is (Spark reads all parquet files)
    if os.path.isdir(path):
        return path

    # If it contains wildcard, expand it and return as list
    if '*' in path or '?' in path:
        files = glob.glob(path)
        if not files:
            raise ValueError(f"No files found matching pattern: {path}")
        # Return list of paths for Spark
        print(files)
        return files  # Spark can handle a list directly
    
    return path


def run_non_optimized(spark, trips_path, zones_path):
    """Non-optimized pipeline: join + groupByKey (multiple shuffles)"""
    print("\n" + "="*60)
    print("RUNNING NON-OPTIMIZED PIPELINE")
    print("="*60)
    
    t_start = time.time()
    
    # Load data (expand wildcards for local files)
    expanded_trips = expand_path(trips_path)
    if isinstance(expanded_trips, list):
        df_trips = spark.read.parquet(*expanded_trips).select(
            "PULocationID", "tpep_pickup_datetime", "fare_amount", "tip_amount"
        )
    else:
        df_trips = spark.read.parquet(expanded_trips).select(
            "PULocationID", "tpep_pickup_datetime", "fare_amount", "tip_amount"
        )
    rdd_trips = df_trips.rdd.map(lambda r: (
        int(r.PULocationID or -1),
        (r.tpep_pickup_datetime, float(r.fare_amount or 0.0), float(r.tip_amount or 0.0))
    ))
    
    df_zones = spark.read.csv(zones_path, header=True)
    rdd_zones = df_zones.rdd.map(lambda r: (int(r['LocationID']), (r['Borough'], r['Zone'])))
    
    t_load = time.time()
    
    # Join (SHUFFLE 1)
    rdd_joined = rdd_trips.join(rdd_zones)
    
    # Compute tip percentage and hour
    rdd_derived = rdd_joined.map(lambda x: (
        x[0],                           # PULocationID
        x[1][1][0],                     # Borough
        x[1][1][1],                     # Zone
        safe_hour(x[1][0][0]),          # hour
        float(x[1][0][1]),              # fare
        float(x[1][0][2]),              # tip
        (float(x[1][0][2]) / float(x[1][0][1]) * 100.0) if x[1][0][1] > 0 else 0.0  # tip_pct
    ))
    
    t_derived = time.time()
    
    # Aggregate by (zone, hour) using groupByKey (SHUFFLE 2)
    rdd_zone_hour = rdd_derived.map(lambda x: ((x[0], x[1], x[2], x[3]), x[6]))
    rdd_grouped = rdd_zone_hour.groupByKey()
    rdd_agg_hour = rdd_grouped.map(lambda x: (
        x[0][0], x[0][1], x[0][2], x[0][3],
        sum(x[1]) / len(list(x[1])),
        len(list(x[1]))
    ))
    
    t_agg_hour = time.time()
    
    # Aggregate across hours using groupByKey (SHUFFLE 3)
    rdd_zone_tmp = rdd_agg_hour.map(lambda x: ((x[0], x[1], x[2]), (x[4], x[5])))
    rdd_grouped_zone = rdd_zone_tmp.groupByKey()
    rdd_agg_zone = rdd_grouped_zone.map(lambda x: (
        x[0][0], x[0][1], x[0][2],
        sum(v[0] for v in x[1]) / len(list(x[1])),
        sum(v[1] for v in x[1])
    ))
    
    t_agg_zone = time.time()
    
    # Sort (SHUFFLE 4)
    rdd_top = rdd_agg_zone.sortBy(lambda x: x[3], ascending=False)
    top_zones = rdd_top.take(20)
    
    t_end = time.time()
    
    print(f"Load time:      {t_load - t_start:.2f}s")
    print(f"Derived:        {t_derived - t_load:.2f}s")
    print(f"Agg by hour:    {t_agg_hour - t_derived:.2f}s")
    print(f"Agg by zone:    {t_agg_zone - t_agg_hour:.2f}s")
    print(f"Sort:           {t_end - t_agg_zone:.2f}s")
    print(f"TOTAL:          {t_end - t_start:.2f}s")
    print("="*60 + "\n")
    
    return {
        "total_time": round(t_end - t_start, 2),
        "top_zones": [(int(r[0]), r[1], r[2], round(r[3], 2), int(r[4])) for r in top_zones]
    }


def run_optimized(spark, trips_path, zones_path):
    """Optimized pipeline: broadcast + reduceByKey + partitioning"""
    print("\n" + "="*60)
    print("RUNNING OPTIMIZED PIPELINE")
    print("="*60)
    
    sc = spark.sparkContext
    t_start = time.time()
    
    # Load trips (expand wildcards for local files)
    expanded_trips = expand_path(trips_path)
    if isinstance(expanded_trips, list):
        df_trips = spark.read.parquet(*expanded_trips).select(
            "PULocationID", "tpep_pickup_datetime", "fare_amount", "tip_amount"
        )
    else:
        df_trips = spark.read.parquet(expanded_trips).select(
            "PULocationID", "tpep_pickup_datetime", "fare_amount", "tip_amount"
        )
    rdd_trips = df_trips.rdd.map(lambda r: (
        int(r.PULocationID or -1),
        (r.tpep_pickup_datetime, float(r.fare_amount or 0.0), float(r.tip_amount or 0.0))
    ))
    
    # Load and broadcast zones (no shuffle join!)
    df_zones = spark.read.csv(zones_path, header=True)
    zones_map = {int(r['LocationID']): (r['Borough'], r['Zone']) for r in df_zones.collect()}
    b_zones = sc.broadcast(zones_map)
    
    t_load = time.time()
    
    # Enrich with broadcast variable
    def enrich_trip(trip):
        pu_id, (pickup_dt, fare, tip) = trip
        hour = safe_hour(pickup_dt)
        tip_pct = (tip / fare * 100.0) if fare > 0 else 0.0
        borough, zone = b_zones.value.get(pu_id, ("Unknown", "Unknown"))
        return ((pu_id, borough, zone, hour), (tip_pct, 1))
    
    # Aggregate by (zone, hour) using reduceByKey (SHUFFLE 1)
    rdd_zone_hour_agg = rdd_trips.map(enrich_trip).reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))
    
    t_agg_hour = time.time()
    
    # Convert to zone-level and partition
    rdd_zone_hour_avg = rdd_zone_hour_agg.map(
        lambda kv: ((kv[0][0], kv[0][1], kv[0][2]), kv[1][0] / kv[1][1])
    )
    
    # Partition by zone and cache
    rdd_zone_hour_avg = rdd_zone_hour_avg.partitionBy(8, lambda k: hash(k[0])).cache()
    _ = rdd_zone_hour_avg.count()  # force cache
    
    t_partition = time.time()
    
    # Aggregate across hours using reduceByKey (SHUFFLE 2)
    rdd_zone_agg = rdd_zone_hour_avg.map(lambda kv: (kv[0], (kv[1], 1))) \
        .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1])) \
        .map(lambda kv: (kv[0][0], kv[0][1], kv[0][2], kv[1][0] / kv[1][1], kv[1][1]))
    
    t_agg_zone = time.time()
    
    # Sort (SHUFFLE 3)
    rdd_top = rdd_zone_agg.sortBy(lambda x: x[3], ascending=False)
    top_zones = rdd_top.take(20)
    
    t_end = time.time()
    
    print(f"Load + broadcast: {t_load - t_start:.2f}s")
    print(f"Agg by hour:      {t_agg_hour - t_load:.2f}s")
    print(f"Partition + cache:  {t_partition - t_agg_hour:.2f}s")
    print(f"Agg by zone:      {t_agg_zone - t_partition:.2f}s")
    print(f"Sort:             {t_end - t_agg_zone:.2f}s")
    print(f"TOTAL:            {t_end - t_start:.2f}s")
    print("="*60 + "\n")
    
    return {
        "total_time": round(t_end - t_start, 2),
        "top_zones": [(int(r[0]), r[1], r[2], round(r[3], 2), int(r[4])) for r in top_zones]
    }


def save_results(result, output_path, job_name):
    """Save results to JSON and CSV"""
    import os
    
    # Create output directory if it doesn't exist
    output_dir = os.path.dirname(output_path)
    if output_dir and not os.path.exists(output_dir):
        os.makedirs(output_dir, exist_ok=True)
    
    with open(f"{output_path}_{job_name}.json", "w") as f:
        json.dump(result, f, indent=2)
    
    with open(f"{output_path}_{job_name}.csv", "w") as f:
        f.write("PULocationID,Borough,Zone,avg_tip_pct,count\n")
        for r in result['top_zones']:
            f.write(f'{r[0]},"{r[1]}","{r[2]}",{r[3]},{r[4]}\n')
    
    print(f"Saved results to {output_path}_{job_name}.[json|csv].")


def main():
    parser = argparse.ArgumentParser(description="NYC Taxi Tip Analysis")
    parser.add_argument("--trips", default="sample_data/yellow_tripdata_2025-01.parquet",
                       help="Path to trips parquet file(s). Supports wildcards (e.g., 'data/*.parquet')")
    parser.add_argument("--zones", default="sample_data/taxi_zone_lookup.csv")
    parser.add_argument("--output", default="output/results")
    parser.add_argument("--job", choices=["1", "2", "both"], default="both",
                       help="1=non-optimized, 2=optimized, both=run both")
    args = parser.parse_args()
    
    print(f"Loading trips from: {args.trips}")
    print(f"Loading zones from: {args.zones}")
    
    spark = create_spark_session("NYC Taxi Analysis")
    
    results = {}
    
    if args.job in ("1", "both"):
        result_non = run_non_optimized(spark, args.trips, args.zones)
        save_results(result_non, args.output, "non_optimized")
        results['non_optimized'] = result_non
    
    if args.job in ("2", "both"):
        result_opt = run_optimized(spark, args.trips, args.zones)
        save_results(result_opt, args.output, "optimized")
        results['optimized'] = result_opt
    
    # Compare if both ran
    if 'non_optimized' in results and 'optimized' in results:
        non_time = results['non_optimized']['total_time']
        opt_time = results['optimized']['total_time']
        speedup = non_time / opt_time
        print(f"\nSpeedup: {speedup:.2f}x (saved {non_time - opt_time:.2f}s)")
    
    spark.stop()


if __name__ == '__main__':
    main()
