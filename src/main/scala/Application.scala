package project

import org.apache.spark.sql.{SparkSession, SaveMode}
import org.apache.spark.sql.types._
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

  /** Safely cast any value to Long */
  def safeLong(value: Any): Long = {
    try {
      value match {
        case i: Int => i.toLong
        case l: Long => l
        case s: String => s.toLong
        case _ => -1L
      }
    } catch {
      case _: Exception => -1L
    }
  }

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

    val customSchema = StructType(Seq(
      StructField("PULocationID", StringType, true),
      StructField("tpep_pickup_datetime", StringType, true),
      StructField("fare_amount", DoubleType, true),
      StructField("tip_amount", DoubleType, true),
    ))
    
    // Load trips
    val dfTrips = spark.read.parquet(tripsPath)
      .schema(customSchema)
      .drop("congestion_surcharge", "airport_fee")
      .select("PULocationID", "tpep_pickup_datetime", "fare_amount", "tip_amount")
    
    // Always treat PULocationID as Long
    val rddTrips: RDD[(Long, (Any, Double, Double))] = dfTrips.rdd.map { r =>
      val id = safeLong(r.get(0))
      val ts = r.get(1)
      val fare = if (r.isNullAt(2)) 0.0 else r.getDouble(2)
      val tip = if (r.isNullAt(3)) 0.0 else r.getDouble(3)
      (id, (ts, fare, tip))
    }
    
    // Load zones and convert LocationID to Long as well
    val dfZones = spark.read.option("header", "true").csv(zonesPath)
    val rddZones: RDD[(Long, (String, String))] = dfZones.rdd.map { r =>
      (safeLong(r.getAs[String]("LocationID")), (r.getAs[String]("Borough"), r.getAs[String]("Zone")))
    }
    
    val tLoad = System.nanoTime()
    
    // Join (both sides Long → safe)
    val rddJoined = rddTrips.join(rddZones)
    
    // Compute tip percentage and hour
    val rddDerived = rddJoined.map { case (puId, ((ts, fare, tip), (borough, zone))) =>
      val hour = safeHour(ts)
      val pct = tipPct(fare, tip)
      (puId, borough, zone, hour, fare, tip, pct)
    }
    
    val tDerived = System.nanoTime()
    
    // Aggregate by (zone, hour)
    val rddZoneHour = rddDerived.map(x => ((x._1, x._2, x._3, x._4), x._7))
    val rddGrouped = rddZoneHour.groupByKey()
    val rddAggHour = rddGrouped.map { case ((puId, borough, zone, hour), tips) =>
      val tipsList = tips.toList
      (puId, borough, zone, hour, tipsList.sum / tipsList.length, tipsList.length)
    }
    
    val tAggHour = System.nanoTime()
    
    // Aggregate across hours
    val rddZoneTmp = rddAggHour.map(x => ((x._1, x._2, x._3), (x._5, x._6)))
    val rddGroupedZone = rddZoneTmp.groupByKey()
    val rddAggZone = rddGroupedZone.map { case ((puId, borough, zone), vals) =>
      val valsList = vals.toList
      val avgTip = valsList.map(_._1).sum / valsList.length
      val totalCount = valsList.map(_._2).sum
      (puId, borough, zone, avgTip, totalCount)
    }
    
    val tAggZone = System.nanoTime()
    
    // Sort
    val rddTop = rddAggZone.sortBy(_._4, ascending = false)
    val topZones = rddTop.take(20).toList
    
    val tEnd = System.nanoTime()
    
    // Print timing
    println(f"TOTAL: ${(tEnd - tStart) / 1e9}%.2fs")
    
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
    
    val customSchema = StructType(Seq(
      StructField("PULocationID", StringType, true),
      StructField("tpep_pickup_datetime", StringType, true),
      StructField("fare_amount", DoubleType, true),
      StructField("tip_amount", DoubleType, true),
    ))

    // Load trips
    val dfTrips = spark.read.parquet(tripsPath)
      .schema(customSchema)
      .drop("congestion_surcharge", "airport_fee")
      .select("PULocationID", "tpep_pickup_datetime", "fare_amount", "tip_amount")
    
    val rddTrips: RDD[(Long, (Any, Double, Double))] = dfTrips.rdd.map { r =>
      val id = safeLong(r.get(0))
      val ts = r.get(1)
      val fare = if (r.isNullAt(2)) 0.0 else r.getDouble(2)
      val tip = if (r.isNullAt(3)) 0.0 else r.getDouble(3)
      (id, (ts, fare, tip))
    }
    
    // Load and broadcast zones
    val dfZones = spark.read.option("header", "true").csv(zonesPath)
    val zonesMap = dfZones.rdd
      .map(r => (safeLong(r.getAs[String]("LocationID")), (r.getAs[String]("Borough"), r.getAs[String]("Zone"))))
      .collect()
      .toMap
    val bZones: Broadcast[Map[Long, (String, String)]] = sc.broadcast(zonesMap)
    
    val tLoad = System.nanoTime()
    
    // Enrich + reduceByKey
    val enrichAndAggregate = rddTrips.map { case (puId, (ts, fare, tip)) =>
      val hour = safeHour(ts)
      val pct = tipPct(fare, tip)
      val (borough, zone) = bZones.value.getOrElse(puId, ("Unknown", "Unknown"))
      ((puId, borough, zone, hour), (pct, 1))
    }
    
    val rddZoneHourAgg = enrichAndAggregate.reduceByKey { case ((sum1, cnt1), (sum2, cnt2)) =>
      (sum1 + sum2, cnt1 + cnt2)
    }
    
    val rddZoneHourAvg = rddZoneHourAgg.map { case ((puId, borough, zone, hour), (sum, count)) =>
      ((puId, borough, zone), sum / count)
    }
    
    val rddPartitioned = rddZoneHourAvg.partitionBy(new HashPartitioner(8))
      .persist(StorageLevel.MEMORY_AND_DISK)
    
    rddPartitioned.count() // Force cache
    
    val rddZoneAgg = rddPartitioned
      .map { case (key, avgTip) => (key, (avgTip, 1)) }
      .reduceByKey { case ((sum1, cnt1), (sum2, cnt2)) => (sum1 + sum2, cnt1 + cnt2) }
      .map { case ((puId, borough, zone), (sum, count)) =>
        (puId, borough, zone, sum / count, count)
      }
    
    val rddTop = rddZoneAgg.sortBy(_._4, ascending = false)
    val topZones = rddTop.take(20).toList
    
    val dfResult = topZones.toDF("PULocationID", "Borough", "Zone", "avg_tip_pct", "count")
    dfResult.coalesce(1).write.mode(SaveMode.Overwrite).option("header", "true").csv(outputPath + "_optimized.csv")
    dfResult.coalesce(1).write.mode(SaveMode.Overwrite).json(outputPath + "_optimized.json")
    println(s"✅ Results saved to ${outputPath}_optimized.[csv|json]")
    
    (System.nanoTime() - tStart) / 1e9
  }

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
    
    if (args.length < 2) {
      println("Usage: Application <deploymentMode> <job>")
      spark.stop()
      return
    }

    val deploymentMode = args(0)
    val job = args(1)
    var writeMode = if (deploymentMode == "sharedRemote") "remote" else deploymentMode

    Commons.initializeSparkContext(deploymentMode, spark)

    val tripsPath =
      if (deploymentMode == "local")
        Commons.getDatasetPath(deploymentMode, "trips/yellow_tripdata_2022-03.parquet")
      else
        Commons.getDatasetPath(deploymentMode, "trips/")
    val zonesPath = Commons.getDatasetPath(deploymentMode, "zones/taxi_zone_lookup.csv")
    val outputPath = Commons.getDatasetPath(writeMode, "output/results")

    if (job == "1") runNonOptimized(spark, tripsPath, zonesPath, outputPath)
    else if (job == "2") runOptimized(spark, tripsPath, zonesPath, outputPath)
    else if (job == "3") runBoth(spark, tripsPath, zonesPath, outputPath)
    else println("Wrong job number. Use 1, 2, or 3")

    spark.stop()
  }
}
