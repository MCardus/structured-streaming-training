import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class KafkaToFsTest extends AnyFlatSpec with Matchers {

  val spark = SparkSession.builder.master("local").getOrCreate()
  import spark.implicits._

  /*def writeToKafka(topic: String, message: String, messageKey: String = ""): Unit = {
    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9099")
    val producer = new KafkaProducer[String, String](props)
    val record   = new ProducerRecord[String, String](topic, messageKey, message)
    producer.send(record)
    producer.close()
  }*/

  "it" should "split lines in words" in {
    val lines = List("Hello dear world", "Have a nice day").toDS()
    lines.show()

    val expectedWords = List("Hello", "dear", "world", "Have", "a", "nice", "day")
    val actualWords   = KafkaToFs.transformStream(lines).collect()

    actualWords shouldBe expectedWords
  }

  /*"it" should "read kafka stream" in {
    val message = "How are you?"
    writeToKafka("testTopic", message)

    val actual = KafkaToFs.readStream("testTopic")
    actual.show()
  }*/
}
