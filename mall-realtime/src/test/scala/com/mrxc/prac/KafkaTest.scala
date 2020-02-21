package com.mrxc.prac

import com.mrxc.Utils.MyKafkaUtil
import com.mrxc.constants.MallConstants
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

object KafkaTest {
  def main(args: Array[String]): Unit = {
     val sparkConf = new SparkConf().setAppName("testKafka").setMaster("local[*]")
    val ssc = new StreamingContext(sparkConf,Seconds(3))

    val kafkaDStream: InputDStream[(String, String)] = MyKafkaUtil.getkafkaStream(ssc,Set(MallConstants.Mall_STARTUP_TOPIC))

    kafkaDStream.map(_._2).print()

    ssc.start()
    ssc.awaitTermination()
  }

}
