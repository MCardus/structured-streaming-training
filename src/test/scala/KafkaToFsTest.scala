import TestUtils.{cleanTemporaryData, writeToKafka}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.util.UUID

class KafkaToFsTest extends AnyFlatSpec with Matchers with BeforeAndAfterEach {

  implicit val spark = SparkSession.builder.master("local").getOrCreate()
  import spark.implicits._

  override def afterEach(): Unit =
    cleanTemporaryData()

  "it" should "split lines in words" in {
    val lines = List("Hello dear world", "Have a nice day").toDS()

    val expectedWords = List("Hello", "dear", "world", "Have", "a", "nice", "day")
    val actualWords   = KafkaToFs.transformStream(lines).collect()

    actualWords shouldBe expectedWords
  }

  "it" should "read kafka stream" in {
    val message       = "Do yo read me?"
    val totalMessages = 100
    val topic         = f"test_topic_${UUID.randomUUID}"
    val inMemoryTable = "test_table"

    (1 to totalMessages).foreach(_ => writeToKafka(topic, message))

    val streamingDataSet = KafkaToFs.readStream(topic, startingOffsets = "earliest")

    streamingDataSet.writeStream
      .format("memory")
      .queryName(inMemoryTable)
      .start()
      .processAllAvailable()

    val actual = spark.sql(f"SELECT * FROM $inMemoryTable").as[String].collect()

    streamingDataSet.isStreaming shouldBe true
    actual.size shouldBe totalMessages
    actual.foreach(row => row shouldBe message)
  }

  "it" should "write a stream to fs" in {
    implicit val sparkCtx = spark.sqlContext
    val message           = "Do yo read me?"
    val totalMessages     = 100
    val outputPath        = "testOutput"

    val sampleStream = MemoryStream[String]
    sampleStream.addData((1 to totalMessages).map(_ => message))
    sampleStream.toDS().isStreaming shouldBe true

    KafkaToFs.writeStream(sampleStream.toDS(), timeout = Some(2500L), path = outputPath)

    val actual = spark.read
      .format("csv")
      .load(f"$outputPath/*.csv")
      .as[String]
      .collect()
    actual.size shouldBe totalMessages
    actual.foreach(row => row shouldBe message)
  }
}
