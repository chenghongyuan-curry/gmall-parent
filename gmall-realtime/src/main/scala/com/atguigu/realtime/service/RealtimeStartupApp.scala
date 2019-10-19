package com.atguigu.realtime.service

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.alibaba.fastjson.JSON
import com.atguigu.common.constants.GmallConstants
import com.atguigu.realtime.bean.StartUpLog
import com.atguigu.realtime.util.{MykafkaUtil, RedesUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis


/**
  * @author chy
  * @date 2019-10-19 12:36
  */
object RealtimeStartupApp {
    def main(args: Array[String]): Unit = {

        val conf: SparkConf = new SparkConf().setAppName("RealtimeStartupApp").setMaster("local[*]")
        val ssc: StreamingContext = new StreamingContext(conf, Seconds(5))


        //TODO 1、消费kafka
        val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MykafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_STARTUP, ssc)


        //TODO 2、数据流 转换 结构变成case class 补充两个时间字段
        val startupLogDStream: DStream[StartUpLog] = kafkaDStream.map { record =>
            val jsonString: String = record.value()
            val startupLog: StartUpLog = JSON.parseObject(jsonString, classOf[StartUpLog])

            val date: Date = new Date(startupLog.ts)
            val dateformat: String = new SimpleDateFormat("yyyy-MM-dd HH").format(date)
            val dateStrings: Array[String] = dateformat.split(" ")

            startupLog.logDate = dateStrings(0)
            startupLog.logHour = dateStrings(1)
            startupLog
        }
        startupLogDStream.print()


        //TODO 3、利用用户清单进行过滤 去重  只保留清单中不存在的用户访问记录
        val filterStartupLogDStream: DStream[StartUpLog] = startupLogDStream.transform { rdd =>
            val jedisClient: Jedis = RedesUtil.getJedisClient

            //得到当前时间日期
            val date: Date = new Date()
            val dataFormat: String = new SimpleDateFormat("yyyy-MM-dd").format(date)
            val dauKey = "dau:" + dataFormat
            //获取redis所有keys
            val allStartupLogMids: util.Set[String] = jedisClient.smembers(dauKey)
            jedisClient.close()

            //将所有keys广播，保证每个driver中都村有一个keys
            val allStartupLogMidsBC: Broadcast[util.Set[String]] = ssc.sparkContext.broadcast(allStartupLogMids)

            //过滤
            println("过滤前：" + rdd.count())
            val filterRDD: RDD[StartUpLog] = rdd.filter { startuplog =>
                val mid: String = startuplog.mid
                val midSet: util.Set[String] = allStartupLogMidsBC.value
                val flag: Boolean = midSet.contains(mid)
                !flag
            }
            println("过滤前：" + filterRDD.count())

            filterRDD
        }



        //TODO 4、批次内进行去重：：按照mid 进行分组，每组取第一个值
        //变成(k,v)结构
        val startupLogGroupByKey: DStream[(String, Iterable[StartUpLog])] = filterStartupLogDStream.map(startuplog=>(startuplog.mid,startuplog)).groupByKey()

        //对v进行排序，取第一个
        val startuplogMap: DStream[(String, List[StartUpLog])] = startupLogGroupByKey.mapValues {
            rdd =>
                rdd.toList.sortWith(
                    (startuplog1, startuplog2) => {
                        startuplog1.logDate < startuplog2.logDate
                    }
                ).take(1)
        }
        //把v扁平化，因为每个list中只有一个元素
        val startuplogFlatMap: DStream[StartUpLog] = startuplogMap.flatMap(_._2)

        //TODO 5、保存今日访问过的用户(mid)清单   -->Redis    1 key类型 ： set    2 key ： dau:2019-xx-xx   3 value : mid
        startuplogFlatMap.foreachRDD{rdd=>
            rdd.foreachPartition{startuplogItr=>
                val jedisClient: Jedis = RedesUtil.getJedisClient

                for (startuplog <- startuplogItr) {
                    val key: String = "dau:" + startuplog.logDate

                    jedisClient.sadd(key,startuplog.mid)
                    println(startuplog)
                }
                jedisClient.close()
            }

        }

        ssc.start()
        ssc.awaitTermination()
    }


}
