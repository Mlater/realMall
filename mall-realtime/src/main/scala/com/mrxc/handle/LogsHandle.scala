package com.mrxc.handle

import com.mrxc.Utils.RedisUtil
import com.mrxc.bean.StartupLog
import org.apache.spark.streaming.dstream.DStream

object LogsHandle {
  def filterSampleBatchMid(filterStepBatchMid: DStream[StartupLog]) = {

    //同批次去重，比如在5秒内点击了4次，但是跨天了，需要按照 天，mid 进行分组，所以先转换格式
    val logTodateMidDStream: DStream[((String, String), StartupLog)] = filterStepBatchMid.map(
      log => ((log.logDate,log.mid),log)
    )

    //按照key分组
    val iterDateMidDStream: DStream[((String, String), Iterable[StartupLog])] = logTodateMidDStream.groupByKey()

    //返回迭代器中时间最小的
    iterDateMidDStream.flatMap{
      case ((_,_),iter) => iter.toList.sortWith(_.ts < _.ts).take(1)
    }
  }

  /**
    * 跨批次去重
    *
    * @param startupLogs
    */
  def filterStepBatchMid(startupLogs: DStream[StartupLog]) = {

    //对startupLogs进行转换，返回值还是DStream
    startupLogs.transform(rdd => {
      //批量过滤
      rdd.mapPartitions(iter => {
        val jedisClient = RedisUtil.getJedisClient

        val logs = iter.filter(logs => {
          val redisKey = s"DAU : ${logs.logDate}"
          !jedisClient.sismember(redisKey, logs.mid)
        })

        jedisClient.close()
        logs
      })
    })

  }

  def saveMidToRedis(startupLogs: DStream[StartupLog]) = {

    startupLogs.foreachRDD(rdd => {
      //批量添加
      rdd.foreachPartition(iter => {
        val jedisClient = RedisUtil.getJedisClient

        //把数据写到redis中
        iter.foreach(log => {
          //把key设置为日期
          val redisKey = s"DAU:${log.logDate}"
          jedisClient.sadd(redisKey, log.mid)
        })

        jedisClient.close()
      })
    })
  }
}
