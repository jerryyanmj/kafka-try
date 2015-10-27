package producer

import java.util.{Date, Properties}

import kafka.producer.{Producer, ProducerConfig, KeyedMessage}
import org.apache.kafka.clients.producer.internals.Partitioner

import scala.util.Random

/**
 * Created by jiarui.yan on 2015-09-11.
 */
object KafkaProducer {


  def main(args: Array[String]) {
    val Array(brokers, topics, events) = args

    val rnd = new Random()

    val properties = new Properties()
    properties.put("metadata.broker.list", brokers)
    properties.put("serializer.class", "kafka.serializer.StringEncoder")
    properties.put("producer.type", "async")

    val kafkaConfig = new ProducerConfig(properties)
    val kafkaProducer = new Producer[String, String](kafkaConfig)

    for (nEvents <- Range(0, events.toInt)) {

      val runtime = new Date().getTime();
      val ip = "192.168.2." + rnd.nextInt(255);
      val msg = runtime + "," + nEvents + ",www.example.com," + ip;
      val data = new KeyedMessage[String, String](topics, ip, msg);
      //println("Sending message " + data)
      kafkaProducer.send(data);
    }

    kafkaProducer.close()
  }
}
