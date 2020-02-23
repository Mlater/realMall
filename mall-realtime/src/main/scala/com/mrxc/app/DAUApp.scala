package com.mrxc.app

import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.JSON
import com.mrxc.Utils.MyKafkaUtil
import com.mrxc.bean.StartupLog
import com.mrxc.constants.MallConstants
import com.mrxc.handle.LogsHandle
import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.phoenix.spark._

object DAUApp {
  def main(args: Array[String]): Unit = {
    val simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH")
    val sparkConf = new SparkConf().setAppName("testKafka").setMaster("local[*]")
    val ssc = new StreamingContext(sparkConf,Seconds(3))

    val kafkaDStream: InputDStream[(String, String)] = MyKafkaUtil.getkafkaStream(ssc,Set(MallConstants.Mall_STARTUP_TOPIC))

    val startupLogs: DStream[StartupLog] = kafkaDStream.map {
      case (_, value) => {
        //转换为json
        val log: StartupLog = JSON.parseObject(value, classOf[StartupLog])

        //取出时间戳
        val ts = log.ts
        //转换格式
        val logDateArr: String = simpleDateFormat.format(new Date(ts))

        //把时间格式转换为数组
        val logDateHourArr = logDateArr.split(" ")

        //获取日期和小时
        log.logDate = logDateHourArr(0)
        log.logHour = logDateHourArr(1)
        log
      }
    }

    //对log进行跨批次去重，和Redis中的数据进行对比
    val filterStepBatchMid = LogsHandle.filterStepBatchMid(startupLogs)

    filterStepBatchMid.cache()

    //对log进行同批次去重，不需要走Redis
    val filterSampleBatchMid = LogsHandle.filterSampleBatchMid(filterStepBatchMid)

    filterSampleBatchMid.cache()

    //把startupLogs写入redis
    LogsHandle.saveMidToRedis(filterSampleBatchMid)


    filterSampleBatchMid.count().print()

    //把处理后的数据写入Hbase
    filterSampleBatchMid.foreachRDD(rdd => {
      rdd.saveToPhoenix(
        "MALL_DAU",
        Seq("MID", "UID", "APPID", "AREA", "OS", "CH", "LOGTYPE", "VS", "LOGDATE", "LOGHOUR", "TS"),
        new Configuration(),
        Some("master102,slaver103,slaver104:2181"))
    })

    ssc.start()
    ssc.awaitTermination()
  }

}
