package com.mrxc.app

import com.alibaba.fastjson.JSON
import com.atguigu.bean.OrderInfo
import com.mrxc.constants.MallConstants
import com.mrxc.Utils.MyKafkaUtil
import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.phoenix.spark._
object GmvApp {
  def main(args: Array[String]): Unit = {

    //创建ssc
    val sparkConf = new SparkConf().setAppName("GmvApp").setMaster("local[*]")
    val ssc = new StreamingContext(sparkConf,Seconds(3))

    //从kafka获取原始数据
    val orgData = MyKafkaUtil.getkafkaStream(ssc,Set(MallConstants.Mall_ORDER_INFO_TOPIC))

    //处理原始数据，数据的目的是要写入Hbase，把数据封装为样例类对象
    val kafkaDStream = orgData.map {
      //现在的数据为k,v类型
      case (_, value) => {
        val orderInfo: OrderInfo = JSON.parseObject(value, classOf[OrderInfo])

        //创建时间的获取
        val create_timeArr = orderInfo.create_time.split(" ")

        //为具体的展示时间赋值
        orderInfo.create_date = create_timeArr(0)
        orderInfo.create_hour = create_timeArr(1).split(":")(0)

        //手机号脱敏
        orderInfo.consignee_tel.splitAt(3)._1 + "********"

        orderInfo
      }
    }

    kafkaDStream.foreachRDD(rdd => {
      rdd.saveToPhoenix("MALL_ORDER_INFO",
        Seq("ID","PROVINCE_ID", "CONSIGNEE", "ORDER_COMMENT", "CONSIGNEE_TEL", "ORDER_STATUS", "PAYMENT_WAY", "USER_ID","IMG_URL", "TOTAL_AMOUNT", "EXPIRE_TIME", "DELIVERY_ADDRESS", "CREATE_TIME","OPERATE_TIME","TRACKING_NO","PARENT_ORDER_ID","OUT_TRADE_NO", "TRADE_BODY", "CREATE_DATE", "CREATE_HOUR"),
        new Configuration(),
        Some("master102,slaver103,slaver104:2181"))
    })

    ssc.start()
    ssc.awaitTermination()
  }

}
