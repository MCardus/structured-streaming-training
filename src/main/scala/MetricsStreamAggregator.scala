import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{Dataset, SparkSession}

import java.sql.Timestamp

case class Metric(eventTime: Timestamp, metric: String)
case class AggregatedMetrics(windowTime: Timestamp, metric: String, count: Long)

object MetricsStreamAggregator {

  /*
   * Exersice: Create a 10 seconds sliding windows every 5 seconds and aggregate results by metric and print it every 5 seconds (use a trigger)
   */

  implicit val spark = SparkSession.builder.master("local[5]").getOrCreate()

  import spark.implicits._
  spark.sparkContext.setLogLevel("WARN")

  def main(args: Array[String]): Unit = {
    val metrics           = readMetricsStream()
    val aggregatedMetrics = aggregateMetrics(metrics)
    writeAggregatedMetrics(aggregatedMetrics)
  }

  def readMetricsStream(): Dataset[Metric] =
    spark.readStream
      .format("rate")
      .option("rowsPerSecond", "10")
      .load()
      .as[(Timestamp, Long)]
      .map { case (eventTime: Timestamp, rowCount: Long) =>
        Metric(eventTime, if (rowCount % 2 == 0) "even" else "odd")
      }

  def aggregateMetrics(metrics: Dataset[Metric]): Dataset[AggregatedMetrics] = ???

  def writeAggregatedMetrics(aggregatedMetrics: Dataset[AggregatedMetrics]): Unit = {
    aggregatedMetrics.writeStream
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()
  }

}
