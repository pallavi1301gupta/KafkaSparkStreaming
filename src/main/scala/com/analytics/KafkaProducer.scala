package com.analytics

import java.util.Properties

import com.google.common.io.Resources
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

/**
  * Kafka Producer
  *
  * Creates a producer and sends messages to Kafka Producer
  */
object KafkaProducer {

  def main(args: Array[String]): Unit = {
    // Creating the producer
    val propsInputStream = Resources.getResource("KafkaProducerProperties.props").openStream
    val properties = new Properties
    properties.load(propsInputStream)
    val producer = new KafkaProducer[String, String](properties)

    // Sending certain messages to Kafka, printing the exception if something fails in sending message to Kafka broker
    try {
      (1 to 1000000).toList.foreach { i => // send lots of messages
        producer.send(new ProducerRecord[String, String]("messages", System.nanoTime.toString))
      }
    } catch {
      case throwable: Throwable =>
        System.out.printf("%s", throwable.getStackTrace)
    } finally {
      // Closing the producer
      producer.close()
    }
  }
}

