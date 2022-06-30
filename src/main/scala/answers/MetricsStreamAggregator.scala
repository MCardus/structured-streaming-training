package answers

import org.apache.spark.sql.functions.window
import org.apache.spark.sql.streaming.StreamingQueryListener.{QueryProgressEvent, QueryStartedEvent, QueryTerminatedEvent}
import org.apache.spark.sql.streaming.{StreamingQueryListener, Trigger}
import org.apache.spark.sql.{Dataset, SparkSession}

import java.sql.Timestamp
import java.time.Instant

case class Metric(eventTime: Timestamp, metric: String)
case class AggregatedMetrics(windowTime: Timestamp, metric: String, count: Long)

object MetricsStreamAggregator {

  implicit val spark = SparkSession.builder.master("local[5]").getOrCreate()

  import spark.implicits._
  spark.sparkContext.setLogLevel("WARN")

  spark.streams.addListener(new StreamingQueryListener() {
    override def onQueryStarted(queryStarted: QueryStartedEvent): Unit = {
      println(f"Query started at ${queryStarted.timestamp}")
    }
    override def onQueryTerminated(queryTerminated: QueryTerminatedEvent): Unit = {
      println("Query terminated: " + queryTerminated.id)
    }
    override def onQueryProgress(queryProgress: QueryProgressEvent): Unit = {
      println(
        f"Query progress: ${queryProgress.progress.inputRowsPerSecond} input rows/s)\nCurrent time: ${new Timestamp(System.currentTimeMillis())}"
      )
    }
  })

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

  def aggregateMetrics(metrics: Dataset[Metric]): Dataset[AggregatedMetrics] = {
    metrics
      .groupBy(
        window($"eventTime", "10 seconds", "5 seconds"),
        $"metric"
      )
      .count()
      .select($"window.start".as("windowTime"), $"metric", $"count")
      .orderBy($"windowTime".asc)
      .as[AggregatedMetrics]

  }

  def writeAggregatedMetrics(aggregatedMetrics: Dataset[AggregatedMetrics]): Unit = {
    aggregatedMetrics.writeStream
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .outputMode("complete")
      .foreachBatch { (batch: Dataset[AggregatedMetrics], batchId: Long) =>
        val now        = Instant.now().toEpochMilli
        val eventsTime = batch.map(_.windowTime).as[Timestamp].collect().toList.map(_.getTime)
        val latency    = if (eventsTime.nonEmpty) now - eventsTime.max else 0L
        batch.show()
        println(s"Query batch latency: $latency miliseconds")
      }
      .start()
      .awaitTermination()
  }

}
