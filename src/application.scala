package project

import org.apache.spark.sql.{SparkSession, SaveMode}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

object Application {

  /** Extracts hour safely from timestamp string */
  def safeHour(ts: String): Int = {
    try {
      ts.substring(11, 13).toInt
    } catch {
      case _: Exception => -1
    }
  }

  /** Compute tip percentage safely */
  def tipPct(fare: Double, tip: Double): Double = {
    if (fare > 0) (tip / fare) * 100.0 else 0.0
  }

  def main(args: Array[String]): Unit = {
    if (args.length < 4) {
      System.err.println("Usage: Application <trips> <zones> <output> <job>")
      System.exit(1)
    }

    val tripsPath = args(0)
    val zonesPath = args(1)
    val outputPath = args(2)
    val job = args(3)

    val spark = SparkSession.builder
      .appName("NYC Taxi Tip Analysis (Scala)")
      .getOrCreate()

    val sc = spark.sparkContext

    println(s"✅ Spark master: ${sc.master}")
    println(s"Loading trips from: $tripsPath")
    println(s"Loading zones from: $zonesPath")
    println(s"Output path: $outputPath")

    import spark.implicits._

    // Load datasets
    val dfTrips = spark.read.parquet(tripsPath)
      .drop("congestion_surcharge", "airport_fee")
      .select("PULocationID", "tpep_pickup_datetime", "fare_amount", "tip_amount")

    val tripsRDD: RDD[(Int, (String, Double, Double))] = dfTrips.rdd.map { r =>
      val id = if (r.isNullAt(0)) -1 else r.getInt(0)
      val ts = if (r.isNullAt(1)) "" else r.getTimestamp(1).toString
      val fare = if (r.isNullAt(2)) 0.0 else r.getDouble(2)
      val tip = if (r.isNullAt(3)) 0.0 else r.getDouble(3)
      (id, (ts, fare, tip))
    }

    val dfZones = spark.read.option("header", "true").csv(zonesPath)
    val zonesMap = dfZones.rdd
      .map(r => (r.getAs[String]("LocationID").toInt, (r.getAs[String]("Borough"), r.getAs[String]("Zone"))))
      .collect()
      .toMap

    val bZones: Broadcast[Map[Int, (String, String)]] = sc.broadcast(zonesMap)

    // Enrich with broadcast zones
    val enriched = tripsRDD.map { case (puId, (ts, fare, tip)) =>
      val (borough, zone) = bZones.value.getOrElse(puId, ("Unknown", "Unknown"))
      val hour = safeHour(ts)
      val pct = tipPct(fare, tip)
      ((puId, borough, zone, hour), (pct, 1))
    }

    // Aggregate by (zone, hour)
    val zoneHourAgg = enriched
      .reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2))
      .map { case ((pu, borough, zone, hour), (sum, count)) =>
        ((pu, borough, zone), (sum / count, count))
      }

    // Aggregate across hours
    val zoneAgg = zoneHourAgg
      .reduceByKey((a, b) => ((a._1 + b._1), (a._2 + b._2)))
      .map { case ((pu, borough, zone), (sum, count)) =>
        (pu, borough, zone, sum / count, count)
      }

    // Sort descending by average tip %
    val topZones = zoneAgg.sortBy(_._4, ascending = false)

    // Convert to DataFrame
    val dfResult = topZones.toDF("PULocationID", "Borough", "Zone", "avg_tip_pct", "count")

    dfResult.coalesce(1)
      .write.mode(SaveMode.Overwrite)
      .option("header", "true")
      .csv(outputPath + "_optimized")

    println(s"✅ Job finished successfully. Results saved to $outputPath")
    spark.stop()
  }
}
