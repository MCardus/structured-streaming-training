import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import java.io.File
import java.time.LocalTime
import java.util.Properties
import scala.reflect.io.Directory

object TestUtils {

  def cleanTemporaryData(): Unit =
    List(
      new Directory(new File("checkpoint")),
      new Directory(new File("testOutput"))
    ).filter(_.exists).foreach(_.deleteRecursively())

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

}
