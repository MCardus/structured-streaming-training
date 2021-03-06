import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{Dataset, SparkSession}

object KafkaToFs {

  implicit val spark = SparkSession.builder.master("local").getOrCreate()
  import spark.implicits._
  spark.sparkContext.setLogLevel("WARN")

  def main(args: Array[String]): Unit = {
    val lines = readStream("myTopic")
    val words = transformStream(lines)
    writeStream(words)
  }

  def readStream(topic: String, port: Int = 9092, startingOffsets: String = "latest")(implicit
      spark: SparkSession
  ): Dataset[String] = {
    spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", f":$port")
      .option("subscribe", topic)
      .option("startingOffsets", startingOffsets)
      .load()
      .select($"value".cast(StringType))
      .as[String]
  }

  def transformStream(lines: Dataset[String]): Dataset[String] =
    lines.flatMap(_.split(" "))

  def writeStream(words: Dataset[String], path: String = "", timeout: Option[Long] = None): Unit =
    words.writeStream
      .format("csv")
      .option("path", path)
      .option("checkpointLocation", "./checkpoint")
      .start()
      .awaitTermination(timeout.getOrElse(Long.MaxValue))
}
