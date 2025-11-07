package project

import org.apache.spark.sql.{SparkSession, SaveMode}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.HashPartitioner
import org.apache.spark.storage.StorageLevel
import utils._

object Application {

  /** Extracts hour safely from timestamp */
  def safeHour(value: Any): Int = {
    try {
      value match {
        case ts: java.sql.Timestamp => ts.toLocalDateTime.getHour
        case s: String if s.length >= 13 => s.substring(11, 13).toInt
        case _ => -1
      }
    } catch {
      case _: Exception => -1
    }
  }

  // def safeLong(value: Any): Long = value match {
  //   case i: Int => i.toLong
  //   case l: Long => l
  //   case s: String => s.toLongOption.getOrElse(-1)
  //   case _ => -1
  // }

  /** Compute tip percentage safely */
  def tipPct(fare: Double, tip: Double): Double = {
    if (fare > 0) (tip / fare) * 100.0 else 0.0
  }

  /** Non-optimized pipeline using groupByKey (multiple shuffles) */
  def runNonOptimized(
    spark: SparkSession,
    tripsPath: String,
    zonesPath: String,
    outputPath: String
  ): Double = {
    
    println("\n" + "=" * 60)
    println("RUNNING NON-OPTIMIZED PIPELINE (Job 1)")
    println("=" * 60)
    
    val sqlContext = spark.sqlContext
    import sqlContext.implicits._
    
    val tStart = System.nanoTime()
    
    // Load trips
    val dfTrips = spark.read.parquet(tripsPath)
      .drop("congestion_surcharge", "airport_fee")
      .select("PULocationID", "tpep_pickup_datetime", "fare_amount", "tip_amount")
    
    val rddTrips: RDD[(Long, (Any, Double, Double))] = dfTrips.rdd.map { r =>
      val id = if (r.isNullAt(0)) -1 else r.getLong(0)
      val ts = r.get(1)
      val fare = if (r.isNullAt(2)) 0.0 else r.getDouble(2)
      val tip = if (r.isNullAt(3)) 0.0 else r.getDouble(3)
      (id, (ts, fare, tip))
    }
    
    // Load zones
    val dfZones = spark.read.option("header", "true").csv(zonesPath)
    val rddZones: RDD[(Long, (String, String))] = dfZones.rdd.map { r =>
      (r.getAs[String]("LocationID").toLong, (r.getAs[String]("Borough"), r.getAs[String]("Zone")))
    }
    
    val tLoad = System.nanoTime()
    
    // Join (SHUFFLE 1)
    val rddJoined = rddTrips.join(rddZones)
    
    // Compute tip percentage and hour
    val rddDerived = rddJoined.map { case (puId, ((ts, fare, tip), (borough, zone))) =>
      val hour = safeHour(ts)
      val pct = tipPct(fare, tip)
      (puId, borough, zone, hour, fare, tip, pct)
    }
    
    val tDerived = System.nanoTime()
    
    // Aggregate by (zone, hour) using groupByKey (SHUFFLE 2)
    val rddZoneHour = rddDerived.map(x => ((x._1, x._2, x._3, x._4), x._7))
    val rddGrouped = rddZoneHour.groupByKey()
    val rddAggHour = rddGrouped.map { case ((puId, borough, zone, hour), tips) =>
      val tipsList = tips.toList
      (puId, borough, zone, hour, tipsList.sum / tipsList.length, tipsList.length)
    }
    
    val tAggHour = System.nanoTime()
    
    // Aggregate across hours using groupByKey (SHUFFLE 3)
    val rddZoneTmp = rddAggHour.map(x => ((x._1, x._2, x._3), (x._5, x._6)))
    val rddGroupedZone = rddZoneTmp.groupByKey()
    val rddAggZone = rddGroupedZone.map { case ((puId, borough, zone), vals) =>
      val valsList = vals.toList
      val avgTip = valsList.map(_._1).sum / valsList.length
      val totalCount = valsList.map(_._2).sum
      (puId, borough, zone, avgTip, totalCount)
    }
    
    val tAggZone = System.nanoTime()
    
    // Sort (SHUFFLE 4)
    val rddTop = rddAggZone.sortBy(_._4, ascending = false)
    val topZones = rddTop.take(20).toList
    
    val tEnd = System.nanoTime()
    
    // Print timing
    println(f"Load time:      ${(tLoad - tStart) / 1e9}%.2fs")
    println(f"Derived:        ${(tDerived - tLoad) / 1e9}%.2fs")
    println(f"Agg by hour:    ${(tAggHour - tDerived) / 1e9}%.2fs")
    println(f"Agg by zone:    ${(tAggZone - tAggHour) / 1e9}%.2fs")
    println(f"Sort:           ${(tEnd - tAggZone) / 1e9}%.2fs")
    println(f"TOTAL:          ${(tEnd - tStart) / 1e9}%.2fs")
    println("=" * 60 + "\n")
    
    // Save results
    val dfResult = topZones.toDF("PULocationID", "Borough", "Zone", "avg_tip_pct", "count")
    dfResult.coalesce(1).write.mode(SaveMode.Overwrite).option("header", "true").csv(outputPath + "_non_optimized.csv")
    dfResult.coalesce(1).write.mode(SaveMode.Overwrite).json(outputPath + "_non_optimized.json")
    println(s"✅ Results saved to ${outputPath}_non_optimized.[csv|json]")
    
    (tEnd - tStart) / 1e9
  }

  /** Optimized pipeline using broadcast + reduceByKey + partitioning */
  def runOptimized(
    spark: SparkSession,
    tripsPath: String,
    zonesPath: String,
    outputPath: String
  ): Double = {
    
    println("\n" + "=" * 60)
    println("RUNNING OPTIMIZED PIPELINE (Job 2)")
    println("=" * 60)
    
    val sqlContext = spark.sqlContext
    import sqlContext.implicits._
    
    val sc = spark.sparkContext
    val tStart = System.nanoTime()
    
    // Load trips
    val dfTrips = spark.read.parquet(tripsPath)
      .drop("congestion_surcharge", "airport_fee")
      .select("PULocationID", "tpep_pickup_datetime", "fare_amount", "tip_amount")
    
    val rddTrips: RDD[(Long, (Any, Double, Double))] = dfTrips.rdd.map { r =>
      val id = if (r.isNullAt(0)) -1 else r.getLong(0)
      val ts = r.get(1)
      val fare = if (r.isNullAt(2)) 0.0 else r.getDouble(2)
      val tip = if (r.isNullAt(3)) 0.0 else r.getDouble(3)
      (id, (ts, fare, tip))
    }
    
    // Load and broadcast zones (no shuffle join!)
    val dfZones = spark.read.option("header", "true").csv(zonesPath)
    val zonesMap = dfZones.rdd
      .map(r => (r.getAs[String]("LocationID").toLong, (r.getAs[String]("Borough"), r.getAs[String]("Zone"))))
      .collect()
      .toMap
    val bZones: Broadcast[Map[Long, (String, String)]] = sc.broadcast(zonesMap)
    
    val tLoad = System.nanoTime()
    
    // Enrich with broadcast variable and aggregate by (zone, hour) using reduceByKey (SHUFFLE 1)
    val enrichAndAggregate = rddTrips.map { case (puId, (ts, fare, tip)) =>
      val hour = safeHour(ts)
      val pct = tipPct(fare, tip)
      val (borough, zone) = bZones.value.getOrElse(puId, ("Unknown", "Unknown"))
      ((puId, borough, zone, hour), (pct, 1))
    }
    
    val rddZoneHourAgg = enrichAndAggregate.reduceByKey { case ((sum1, cnt1), (sum2, cnt2)) =>
      (sum1 + sum2, cnt1 + cnt2)
    }
    
    val tAggHour = System.nanoTime()
    
    // Convert to zone-level and partition
    val rddZoneHourAvg = rddZoneHourAgg.map { case ((puId, borough, zone, hour), (sum, count)) =>
      ((puId, borough, zone), sum / count)
    }
    
    // Partition by zone and cache
    val rddPartitioned = rddZoneHourAvg.partitionBy(new HashPartitioner(8))
      .persist(StorageLevel.MEMORY_AND_DISK)
    
    rddPartitioned.count() // Force cache
    
    val tPartition = System.nanoTime()
    
    // Aggregate across hours using reduceByKey (SHUFFLE 2)
    val rddZoneAgg = rddPartitioned
      .map { case (key, avgTip) => (key, (avgTip, 1)) }
      .reduceByKey { case ((sum1, cnt1), (sum2, cnt2)) => (sum1 + sum2, cnt1 + cnt2) }
      .map { case ((puId, borough, zone), (sum, count)) =>
        (puId, borough, zone, sum / count, count)
      }
    
    val tAggZone = System.nanoTime()
    
    // Sort (SHUFFLE 3)
    val rddTop = rddZoneAgg.sortBy(_._4, ascending = false)
    val topZones = rddTop.take(20).toList
    
    val tEnd = System.nanoTime()
    
    // Print timing
    println(f"Load + broadcast: ${(tLoad - tStart) / 1e9}%.2fs")
    println(f"Agg by hour:      ${(tAggHour - tLoad) / 1e9}%.2fs")
    println(f"Partition + cache:  ${(tPartition - tAggHour) / 1e9}%.2fs")
    println(f"Agg by zone:      ${(tAggZone - tPartition) / 1e9}%.2fs")
    println(f"Sort:             ${(tEnd - tAggZone) / 1e9}%.2fs")
    println(f"TOTAL:            ${(tEnd - tStart) / 1e9}%.2fs")
    println("=" * 60 + "\n")
    
    // Save results
    val dfResult = topZones.toDF("PULocationID", "Borough", "Zone", "avg_tip_pct", "count")
    dfResult.coalesce(1).write.mode(SaveMode.Overwrite).option("header", "true").csv(outputPath + "_optimized.csv")
    dfResult.coalesce(1).write.mode(SaveMode.Overwrite).json(outputPath + "_optimized.json")
    println(s"✅ Results saved to ${outputPath}_optimized.[csv|json]")
    
    (tEnd - tStart) / 1e9
  }

  /** Run both pipelines and compare */
  def runBoth(
    spark: SparkSession,
    tripsPath: String,
    zonesPath: String,
    outputPath: String
  ): Unit = {
    println("\n" + "=" * 60)
    println("RUNNING BOTH PIPELINES (Job 3)")
    println("=" * 60 + "\n")
    
    val timeNonOpt = runNonOptimized(spark, tripsPath, zonesPath, outputPath)
    val timeOpt = runOptimized(spark, tripsPath, zonesPath, outputPath)
    
    val speedup = timeNonOpt / timeOpt
    val saved = timeNonOpt - timeOpt
    
    println("\n" + "=" * 60)
    println("COMPARISON RESULTS")
    println("=" * 60)
    println(f"Non-optimized time: $timeNonOpt%.2fs")
    println(f"Optimized time:     $timeOpt%.2fs")
    println(f"Speedup:            $speedup%.2fx")
    println(f"Time saved:         $saved%.2fs")
    println("=" * 60 + "\n")
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("NYC Taxi Tip Analysis")
      .config("spark.sql.session.timeZone", "UTC")
      .getOrCreate()
    
    val sqlContext = spark.sqlContext
    import sqlContext.implicits._

    if (args.length < 2) {
      println("Usage: Application <deploymentMode> <job>")
      println("  deploymentMode: \"local\", \"remote\", or \"sharedRemote\"")
      println("  job: 1 (non-optimized), 2 (optimized), or 3 (both)")
      spark.stop()
      return
    }

    val deploymentMode = args(0)
    var writeMode = deploymentMode
    if (deploymentMode == "sharedRemote") {
      writeMode = "remote"
    }
    val job = args(1)

    // Initialize Spark context with AWS credentials if needed
    Commons.initializeSparkContext(deploymentMode, spark)

    // Get dataset paths
    val tripsPath = Commons.getDatasetPath(deploymentMode, "trips/yellow_tripdata_2022-03.parquet")
    val zonesPath = Commons.getDatasetPath(deploymentMode, "zones/taxi_zone_lookup.csv")
    val outputPath = Commons.getDatasetPath(writeMode, "output/results")
println(s"Deployment mode: $deploymentMode")
    println(s"Trips path: $tripsPath")
    println(s"Zones path: $zonesPath")
    println(s"Output path: $outputPath")

    if (job == "1") {
      runNonOptimized(spark, tripsPath, zonesPath, outputPath)
    }
    else if (job == "2") {
      runOptimized(spark, tripsPath, zonesPath, outputPath)
    }
    else if (job == "3") {
      runBoth(spark, tripsPath, zonesPath, outputPath)
    }
    else {
      println("Wrong job number. Use 1, 2, or 3")
    }

    spark.stop()
  }
}
