package com.mrxc.Utils

import java.util.Properties

import kafka.serializer.StringDecoder
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.KafkaUtils

object MyKafkaUtil {
  def getkafkaStream(ssc: StreamingContext, topics: Set[String]) = {
    val properties: Properties = PropertiesUtil.load("config.properties")

    val kafkaPara = Map(
      "bootstrap.servers" -> properties.getProperty("kafka.broker.list"),
      "group.id" -> "mall"
    )

    val kafkaDStream: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String,String,StringDecoder,
      StringDecoder](ssc,
      kafkaPara,
      topics)

    kafkaDStream

  }

}
