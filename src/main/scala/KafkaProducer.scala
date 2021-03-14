import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import java.text.SimpleDateFormat
import java.util.{Calendar, Properties, UUID}
import scala.io.Source

object KafkaProducer {

  def main(args: Array[String]): Unit ={

    val properties = new Properties()
    properties.put("bootstrap.servers", "localhost:9092")
    properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val producer  = new KafkaProducer[String, String](properties)

    val FileStream = Source.fromFile("/Users/josemorgado/Documents/Untitled Folder/data/customised.csv")

    for (line <- FileStream.getLines.drop(1)) {

      val today = Calendar.getInstance.getTime
      val formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

      val intermediary: Array[String] = line.split(",").map(_.trim)

      val key = intermediary(1)
      val value = intermediary.drop(1).mkString(",").replaceAll("(\\d{4})-(\\d{2})-(\\d{2}) (\\d{2}):(\\d{2}):(\\d{2})", formatter.format(today))

      Thread.sleep(5000)

      val data = new ProducerRecord[String, String]("my-topic", key, value)

      producer.send(data)

  }





  }



}
