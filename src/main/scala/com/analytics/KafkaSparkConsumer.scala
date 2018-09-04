package com.analytics

import io.netty.channel.ChannelOutboundBuffer.MessageProcessor
import kafka.serializer.{DefaultDecoder, StringDecoder}
import org.apache.spark.SparkConf
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.util.Random

/**
  * Kafka Spark Consumer
  *
  * Consumes messages from Kafka broker and writes in a text file in a fault tolerant manner
  */
object KafkaSparkConsumer extends MessageProcessor {

  def main(args: scala.Array[String]): Unit = {

    // Creating StreamingContext using the checkpoint if it exists
    val streamingContext = StreamingContext
      .getOrCreate("file:///home/kafkaSparkStreaming/sparkStreamingCheckPoint", functionToCreateContext)

    streamingContext.start()
    streamingContext.awaitTermination()
  }

  /**
    * functionToCreateContext
    * @return StreamingContext
    */
  def functionToCreateContext(): StreamingContext = {
    // Creating spark configuration
    val sparkConf = new SparkConf().setAppName("SaveExactlyOnce").setMaster("local[*]")

    //Creating streaming context using the spark configuration
    val streamingContext = new StreamingContext(sparkConf, Seconds(10))
    val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    streamingContext.checkpoint("file:///home/kafkaSparkStreaming/sparkStreamingCheckPoint")
    consumeMessageFromKafka(streamingContext, sparkSession)
    streamingContext
  }

  /**
    * consumeMessageFromKafka
    * @param streamingContext
    * @param sparkSession
    */
  def consumeMessageFromKafka(streamingContext: StreamingContext, sparkSession: SparkSession): Unit = {

    //Assigning a random group id for consumer such as group-d5ed
    val groupId = "group-" + Random.alphanumeric.take(4).mkString("")

    // Setting up properties for conusmer
    val kafkaParams = Map(
      "metadata.broker.list" -> "localhost:9092",
      "zookeeper.connect" -> "localhost:2181",
      "group.id" -> groupId,
      "auto.offset.reset" -> "smallest",
      "zookeeper.session.timeout" -> "123123123")

    // Creating DStream using default and string decoder of Kafka
    val messageStream = KafkaUtils.createStream[scala.Array[Byte], String, DefaultDecoder, StringDecoder](
      streamingContext,
      kafkaParams,
      Map("messages" -> 1),
      StorageLevel.MEMORY_ONLY_SER
    ).map(_._2)

    // Defining the schema for the message
    val messageSchema = StructType(Array(StructField("timestamp_message", StringType)))

    // Saving each record to a text file
    messageStream.foreachRDD { messageRDD =>
      val sqlContext = SparkSession.builder.config(messageRDD.sparkContext.getConf).getOrCreate().sqlContext
      val messageRowRDD = messageRDD.map(y => Row.fromSeq(Seq(y)))
      if (messageRDD != null) {
        sqlContext.createDataFrame(messageRowRDD, messageSchema).coalesce(1)
          .write.mode(SaveMode.Append).text("file:///home/kafkaSparkStreaming/outputFile")
      }
    }
  }

  /**
    * processMessage
    * @param message
    * @return
    */
  override def processMessage(message: scala.Any): Boolean = true

}