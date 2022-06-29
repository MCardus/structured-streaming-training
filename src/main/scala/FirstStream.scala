import org.apache.spark.sql.SparkSession

object FirstStream extends App {

  val spark = SparkSession.builder.master("local").getOrCreate()
  import spark.implicits._
  spark.sparkContext.setLogLevel("WARN")

  val lines = spark.readStream
    .format("socket")
    .option("host", "localhost")
    .option("port", 9999)
    .load()
    .as[String]

  val words = lines.flatMap(_.split(" "))

  words.writeStream
    .format("console")
    .start()
    .awaitTermination()
}
