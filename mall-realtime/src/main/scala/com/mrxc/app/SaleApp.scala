package com.mrxc.app

import com.alibaba.fastjson.JSON
import com.atguigu.bean.OrderInfo
import com.mrxc.Utils.{MyESUtil, MyKafkaUtil, RedisUtil}
import com.mrxc.bean.{OrderDetail, SaleDetailWide, UserInfo}
import com.mrxc.constants.MallConstants
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization

import scala.collection.mutable.ListBuffer

object SaleApp {
  def main(args: Array[String]): Unit = {

    //先来消费这3个kafka中的数据
    //获取连接
    val sparkConf = new SparkConf().setAppName("SaleApp").setMaster("local[*]")
    val ssc = new StreamingContext(sparkConf, Seconds(3))

    //获取kafka中这3个主题的数据
    val order_info_KafkaDStream = MyKafkaUtil.getkafkaStream(ssc, Set(MallConstants.Mall_ORDER_INFO_TOPIC))
    val order_datail_KafkaDStream = MyKafkaUtil.getkafkaStream(ssc, Set(MallConstants.Mall_ORDER_DETAIL_TOPIC))
    val user_info_KafkaDStream = MyKafkaUtil.getkafkaStream(ssc, Set(MallConstants.Mall_USER_INFO_TOPIC))

    //把用户数据直接存到Redis中
    user_info_KafkaDStream.foreachRDD(rdd => {
      //每个分区获取一次连接
      rdd.foreachPartition(iter => {
        val jedisClient = RedisUtil.getJedisClient
        //一个一个添加用户
        iter.foreach{
          case (_,value) => {
            val userInfo = JSON.parseObject(value,classOf[UserInfo])
            //redis存储采用String类型
            jedisClient.set(s"user:${userInfo.id}",value)
          }
        }
        //释放连接
        jedisClient.close()
      })
    })

    //把数据封装为样例类对象，并且转换为(k,v)结构，order_id为k，方便之后进行join
    val id_orderinfo_DStream = order_info_KafkaDStream.map {
      case (_, value) => {
        val orderInfo = JSON.parseObject(value, classOf[OrderInfo])
        //为样例类中定义的时间赋值，处理创建日期及小时 2020-02-21 12:12:12
        val createTimeArr: Array[String] = orderInfo.create_time.split(" ")
        orderInfo.create_date = createTimeArr(0)
        orderInfo.create_hour = createTimeArr(1).split(":")(0)
        //手机号脱敏
        orderInfo.consignee_tel = orderInfo.consignee_tel.splitAt(4)._1 + "*******"

        //把结果返回
        orderInfo
      }
    }.map(orderinfo => (orderinfo.id, orderinfo))

    //把订单详情表进行封装
    val order_id_orderDetail_DStream = order_datail_KafkaDStream.map {
      case (_, value) => {
        val orderDetail = JSON.parseObject(value, classOf[OrderDetail])
        orderDetail
      }
    }.map(orderDetail => (orderDetail.order_id, orderDetail))

    //对订单表和订单详情表进行join操作，这个join是join的当前批次的，但是有可能有网络延迟的问题
    val joinDStream = id_orderinfo_DStream.fullOuterJoin(order_id_orderDetail_DStream)

    //所以我们把order_id做缓存，没匹配上的订单详情数据也缓存起来

    val orderInfoAndDetailDStream = joinDStream.mapPartitions(iter => {
      //
      val listBuffer = new ListBuffer[SaleDetailWide]()

      //每个分区获取一次rdis连接
      val jedisClient = RedisUtil.getJedisClient

      implicit val format: DefaultFormats.type = org.json4s.DefaultFormats

      iter.foreach {
        case (orderid, (orderInfoOpt, orderDetailOpt)) => {
          //先定义存放在Redis中的RedisKey
          val orderIdRedisKey = s"order:$orderid"
          val orderDetailRedisKey = s"order_detail:$orderid"

          //判断join后的数据中，orderInfo是否为空，如果不为空，就把detail缓存起来
          //首先join后的结果有3种情况，
          // orderinfo不为空,orderdetail不为空，写入这个宽表，保存orderinfo
          // orderinfo不为空,orderdetail为空，orderinfo缓存起来
          // orderinfo为空,orderdetail不为空，orderdetail缓存起来
          if (orderInfoOpt.isDefined) {
            val orderInfo = orderInfoOpt.get

            if (orderDetailOpt.isDefined) {
              val orderDetail = orderDetailOpt.get
              //orderinfo不为空,orderdetail不为空，写入这个宽表
              listBuffer += new SaleDetailWide(orderInfo, orderDetail)
            }

            //orderinfo不为空,orderdetail为空，orderinfo缓存起来
            val orderJson = Serialization.write(orderInfo)
            //把这个order保存在redis
            jedisClient.set(orderIdRedisKey, orderJson)
            //设置保存时间为300s，300s后就删除这个K
            jedisClient.expire(orderIdRedisKey, 300)

            //查询这个orderInfo对应的detail缓存
            val orderDetailSet = jedisClient.smembers(orderDetailRedisKey)
            import scala.collection.JavaConversions._
            orderDetailSet.foreach(detailJson => {
              //遍历detail缓存
              //既然能遍历出来数据，就说明能匹配上
              val detail = JSON.parseObject(detailJson, classOf[OrderDetail])
              listBuffer += new SaleDetailWide(orderInfo, detail)
            })
          } else {
            //orderinfo为空,orderdetail不为空，orderdetail缓存起来
            val orderDetail = orderDetailOpt.get

            //查询orderInfo的缓存，看有没有能匹配的上的，如果有就写出，没有就缓存起来
            if (jedisClient.exists(orderIdRedisKey)) {
              val orderJson = jedisClient.get(orderIdRedisKey)
              //把JSON对象转为OrderInfo对象
              val orderInfo: OrderInfo = JSON.parseObject(orderJson, classOf[OrderInfo])

              listBuffer += new SaleDetailWide(orderInfo, orderDetail)
            } else {
              //没有缓存的话就先把这个detail缓存起来
              val detailJson = Serialization.write(orderDetail)
              jedisClient.sadd(orderDetailRedisKey, detailJson)
              jedisClient.expire(orderDetailRedisKey, 300)
            }
          }
        }
      }

      jedisClient.close()
      listBuffer.toIterator
    })

    //把用户信息也封装到宽表中
    val finalSaleDetailWide = orderInfoAndDetailDStream.mapPartitions(iter => {
      //分区操作，获取连接
      val jedisClient = RedisUtil.getJedisClient

      val result = iter.map(saleDetailWide => {
        //在redis中查对应的用户信息
        val userJson = jedisClient.get(s"user:${saleDetailWide.user_id}")
        //把Json格式转为样例类对象
        val userInfo: UserInfo = JSON.parseObject(userJson, classOf[UserInfo])
        //把用户信息合并到宽表中
        saleDetailWide.mergeUserInfo(userInfo)
        saleDetailWide
      })
      jedisClient.close()
      result
    })
    //做缓存
    finalSaleDetailWide.cache()

    //测试打印
    finalSaleDetailWide.print(100)

    //写入ES
    finalSaleDetailWide.foreachRDD(rdd => {
      rdd.foreachPartition(iter => {
        val tuples = iter.map(saleDetailWide => {
          (s"${saleDetailWide.order_id}-${saleDetailWide.order_detail_id}", saleDetailWide)
        })

        MyESUtil.insertBulk(MallConstants.Mall_SALE_DETAIL_INDEX,tuples.toList)
      })
    })

    //启动任务
    ssc.start()
    ssc.awaitTermination()
  }
}
