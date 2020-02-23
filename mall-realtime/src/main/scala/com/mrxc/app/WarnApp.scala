package com.mrxc.app

import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.JSON
import com.mrxc.Utils.MyKafkaUtil
import com.mrxc.bean.{EventLog, StartupLog, WarnLog}
import com.mrxc.constants.MallConstants
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import java.util

import scala.util.control.Breaks._

object WarnLogApp {
  def main(args: Array[String]): Unit = {

    //创建ssc
    val sparkConf = new SparkConf().setAppName("WarnLogApp").setMaster("local[*]")
    val ssc = new StreamingContext(sparkConf,Seconds(5))

    //从kafka的事件主题中获取数据
    val kafkaDStream: InputDStream[(String, String)] = MyKafkaUtil.getkafkaStream(ssc,Set(MallConstants
      .Mall_EVENT_TOPIC))

    val simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH")
    val eventDstream: DStream[EventLog] = kafkaDStream.map {
      case (_, value) => {
        val eventLog = JSON.parseObject(value, classOf[EventLog])

        //转换为日期的时间
        val dateArr = simpleDateFormat.format(new Date(eventLog.ts))

        eventLog.logDate = dateArr.split(" ")(0)
        eventLog.logHour = dateArr.split(" ")(1)

        eventLog
      }
    }

    //同一设备（分组）30秒内（窗口） 三次不同账号登录（用户）领取优惠券（行为）没有浏览商品（行为）
    //同一设备每分钟只记录一次预警（去重）
    val windowEventLogDStream = eventDstream.window(Seconds(30))

    //对同一个mid的日志进行分组
    val midToGroup = windowEventLogDStream
      .map(log => (log.mid, log))
      .groupByKey()

    //筛选出预警的日志
    //满足条件：同一设备（分组）30秒内（窗口） 三次不同账号登录（用户）领取优惠券（行为）没有浏览商品（行为）
    //现在相同mid的日志已经被分为1组，那么遍历看后面的条件是否满足，需要返回值，所以选择map
    val WarnJSON = midToGroup.map {
      case (mid, iter) =>

        //用户id
        val uids = new util.HashSet[String]()
        //恶意点击的商品id
        val goods = new util.HashSet[String]()
        //事件行为
        val events = new util.ArrayList[String]()

        //默认该条日志没有浏览
        var noClick: Boolean = true
        breakable {
          //遍历
          //不论如何先添加日志的事件行为
          // 如果日志中有点击行为，就不属于警告
          //如果日志中有领取优惠券行为，就记录用户id和优惠券对应的商品id
          iter.foreach(log => {
            events.add(log.evid)
            if ("clickItem".equals(log.evid)) {
              noClick = false
              //如果有浏览行为就退出本次循环
              break
            } else if ("coupon".equals(log.evid)) {
              uids.add(log.uid)
              goods.add(log.itemid)
            }
          })
        }
        //封装预警日志
        /*if (uids.size() >= 3 && noClick) {
          WarnLog(mid, uids, goods, events, System.currentTimeMillis())
        }*/
        (uids.size() >= 3 && noClick,WarnLog(mid, uids, goods, events, System.currentTimeMillis()))

    }

    //WarnJSON.print()
    WarnJSON.filter(_._1).map(_._2).print()

    //启动任务
    ssc.start()
    ssc.awaitTermination()
  }

}
