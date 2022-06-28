import org.apache.spark.sql.SparkSession

object FirstStream {

  def apply(): Unit = {

    val spark = SparkSession.builder.getOrCreate()

    val lines = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 9999)
      .load()

    import spark.implicits._
    val words = lines.as[String].flatMap(_.split(" "))

    words.writeStream
      .format("console")
      .start()
      .awaitTermination()
  }
}
