import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.time.LocalTime
import java.util.{Properties, UUID}

class KafkaToFsTest extends AnyFlatSpec with Matchers {

  implicit val spark = SparkSession.builder.master("local").getOrCreate()
  import spark.implicits._

  def writeToKafka(topic: String, message: String): Unit = {
    val props = new Properties()
    props.put("bootstrap.servers", ":9092")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    val producer = new KafkaProducer[String, String](props)
    val record   = new ProducerRecord[String, String](topic, LocalTime.now().toString, message)
    producer.send(record)
    producer.close()
  }

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
}
