
import java.util.Properties
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

/**
  * This object just populates the input kafka topic
  */
object SendDataToKakfa extends App{

  val inputTopic = "inputTopic"
  val broker = "localhost:9092"

  val properties = new Properties()
  properties.put("bootstrap.servers", broker)
  properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

  val producer = new KafkaProducer[String, String](properties)
  val message =" >>>>>>>>>>>>>>>>>>>>>>>>> Data Message"
  var key = 0
  while (key < 200) {
    key = key + 1
    val record = new ProducerRecord[String, String](inputTopic, (message + s" $key"))
    producer.send(record).get().toString
    println(s"inserted data : $key ")
  }
producer.close()
}
