package com.atguigu.realtime.service

import java.util

import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.common.constants.GmallConstants
import com.atguigu.realtime.bean.{OrderDetail, OrderInfo, SaleDetail, UserInfo}
import com.atguigu.realtime.util.{MyESUtil, MykafkaUtil, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

import scala.collection.mutable.ListBuffer
import org.json4s.native.Serialization

/**
  * @author chy
  * @date 2019-10-27 12:48
  */
object SaleApp {
    def main(args: Array[String]): Unit = {

        val conf: SparkConf = new SparkConf().setAppName("SaleApp").setMaster("local[*]")
        val ssc: StreamingContext = new StreamingContext(conf, Seconds(5))

        val orderInfoInputDStream: InputDStream[ConsumerRecord[String, String]] = MykafkaUtil.getKafkaStream(GmallConstants.KAFKA_ORDER, ssc)
        val orderDetailInputDStream: InputDStream[ConsumerRecord[String, String]] = MykafkaUtil.getKafkaStream(GmallConstants.KAFKA_ORDER_DETAIL, ssc)

        //把两个流转换为case class
        //补充对应字段
        val orderInfoDstream: DStream[OrderInfo] = orderInfoInputDStream.map {
            record => {
                val jsonString: String = record.value()
                val orderInfo: OrderInfo = JSON.parseObject(jsonString, classOf[OrderInfo])

                //脱敏
                val consignee_tel: String = orderInfo.consignee_tel
                val from3last8: (String, String) = consignee_tel.splitAt(3)
                val from3: String = from3last8._1
                val last4: String = from3last8._2.splitAt(4)._2
                orderInfo.consignee_tel = from3 + "****" + last4

                //补充时间日期
                val create_time: String = orderInfo.create_time
                val dataAndHour: Array[String] = create_time.split(" ")
                orderInfo.create_date = dataAndHour(0)
                orderInfo.create_hour = dataAndHour(1).split(":")(0)
                orderInfo
            }

        }

        val orderDetailDstream: DStream[OrderDetail] = orderDetailInputDStream.map {
            record => {
                val jsonString: String = record.value()
                val orderDetail: OrderDetail = JSON.parseObject(jsonString, classOf[OrderDetail])
                orderDetail
            }
        }


        //转换为kv结构才能使用join
        val orderInfoDstreamWithKey: DStream[(String, OrderInfo)] = orderInfoDstream.map(orerinfo => (orerinfo.id, orerinfo))
        val OrderDetailDstreamWithKey: DStream[(String, OrderDetail)] = orderDetailDstream.map(orderdetail => (orderdetail.order_id, orderdetail))

        //使用单join会丢数据
        /*val joinDstream: DStream[(String, (OrderInfo, OrderDetail))] = orderInfoDstreamWithKey.join(OrderDetailDstreamWithKey)

        joinDstream.foreachRDD(
            rdd => {
                rdd.foreach {
                    case (orderId, (orderInfo, orderDetail)) =>
                        println(orderId + "||" + orderInfo + "||" + orderDetail)
                }
            }
        )*/


        //双流join
        val fulljoinDstream: DStream[(String, (Option[OrderInfo], Option[OrderDetail]))] = orderInfoDstreamWithKey.fullOuterJoin(OrderDetailDstreamWithKey)

        val saleDetailDstream: DStream[SaleDetail] = fulljoinDstream.mapPartitions {
            fulljoinResultItr => {
                //定义一个saleDetail的集合，把成功join的结果放进集合，最后返回
                val saleDetailList: ListBuffer[SaleDetail] = ListBuffer[SaleDetail]()
                for ((orderId, (orderInfoOpt, orderDetailOpt)) <- fulljoinResultItr) {
                    if (orderInfoOpt != None) {
                        val orderInfo: OrderInfo = orderInfoOpt.get
                        //判断orderdetail,如果不为空，生成一个saledetail
                        if (orderDetailOpt != None) {
                            val orderDetail: OrderDetail = orderDetailOpt.get
                            val saleDetail: SaleDetail = new SaleDetail(orderInfo, orderDetail)
                            saleDetailList += saleDetail
                        }

                        //把自己的数据放进缓存
                        //redis type:String key:(order_info:[orderId]) value:order_info
                        val jedisClient: Jedis = RedisUtil.getJedisClient
                        val orderInfoKey: String = "order_info:" + orderInfo.id

                        implicit val formats = org.json4s.DefaultFormats
                        val orderInfoValue: String = Serialization.write(orderInfo)

                        jedisClient.setex(orderInfoKey, 600, orderInfoValue)

                        //查找缓存，看是否还有能够匹配的orderdetail
                        val orderDetailKey: String = "order_detail:" + orderInfo.id
                        val orderDetailSet: util.Set[String] = jedisClient.smembers(orderDetailKey)
                        if (orderDetailSet != null && orderDetailSet.size() > 0) {
                            import scala.collection.JavaConversions._
                            for (orderDetailJson <- orderDetailSet) {
                                val orderDetail: OrderDetail = JSON.parseObject(orderDetailJson, classOf[OrderDetail])
                                val saleDetail: SaleDetail = new SaleDetail(orderInfo, orderDetail)
                                saleDetailList += saleDetail
                            }
                        }
                        jedisClient.close()
                    } else {
                        //orderInfo为空，要将orderDetail先放进redis中，等着orderInfo匹配
                        //1.写缓存
                        val orderDetail: OrderDetail = orderDetailOpt.get
                        val jedisClient: Jedis = RedisUtil.getJedisClient

                        val orderDetailKey: String = "order_detail:" + orderDetail.order_id

                        implicit val formats = org.json4s.DefaultFormats
                        val orderDetailJson: String = Serialization.write(orderDetail)
                        jedisClient.sadd(orderDetailKey, orderDetailJson)
                        jedisClient.expire(orderDetailKey, 600)

                        //2.读缓存
                        val orderInfoKey: String = "order_info:" + orderDetail.order_id
                        val orderInfoJson: String = jedisClient.get(orderInfoKey)
                        //如果找到orderInfo就生成saleDetail,放进结果集中
                        if (orderInfoJson != null && orderInfoJson.size > 0) {
                            val orderInfo: OrderInfo = JSON.parseObject(orderInfoJson, classOf[OrderInfo])
                            val saleDetail: SaleDetail = new SaleDetail(orderInfo, orderDetail)
                            saleDetailList += saleDetail
                        }
                        jedisClient.close()
                    }
                }
                //转为Iter返回
                saleDetailList.toIterator
            }
        }


        //把同步到redis中的userInfo补全到SaleDetail中
        val wonderfulSaleDetail: DStream[SaleDetail] = saleDetailDstream.mapPartitions { saleDetailItr =>
            val jedisClient: Jedis = RedisUtil.getJedisClient
            val saleDetailList: ListBuffer[SaleDetail] = ListBuffer[SaleDetail]()
            for (saleDetail <- saleDetailItr) {
                val userInfoKey: String = "user_info:" + saleDetail.user_id
                val userInfoJson: String = jedisClient.get(userInfoKey)
                val userInfo: UserInfo = JSON.parseObject(userInfoJson, classOf[UserInfo])
                saleDetail.mergeUserInfo(userInfo)
                saleDetailList += saleDetail
            }
            jedisClient.close()
            saleDetailList.toIterator
        }

        wonderfulSaleDetail.foreachRDD { rdd =>
            val saleDetailList: List[SaleDetail] = rdd.collect().toList
            val saleDetailWithKeyList: List[(String, SaleDetail)] = saleDetailList.map(saleDetail => (saleDetail.order_detail_id, saleDetail))
            MyESUtil.insertBulk( saleDetailWithKeyList,GmallConstants.ES_INDEX_SALE_DETAIL,GmallConstants.ES_TYPE_DEFAULT)

        }


        wonderfulSaleDetail.foreachRDD(
            rdd => println(rdd.collect().mkString("\n"))
        )


        //同步userInfo到redis库
        val userInfoDstream: InputDStream[ConsumerRecord[String, String]] = MykafkaUtil.getKafkaStream(GmallConstants.KAFKA_USER, ssc)
        //redis type:String key:(user_info:[user_id]) value:user_info
        val userInfoInputDstream: DStream[UserInfo] = userInfoDstream.map {
            record => {
                val userInfoJson: String = record.value()
                val userInfo: UserInfo = JSON.parseObject(userInfoJson, classOf[UserInfo])
                userInfo
            }
        }

        userInfoInputDstream.foreachRDD {
            rdd => {
                rdd.foreachPartition(userInfoItr => {
                    val jedisClient: Jedis = RedisUtil.getJedisClient

                    for (userInfo <- userInfoItr) {
                        implicit val formats = org.json4s.DefaultFormats
                        val userInfoJson: String = Serialization.write(userInfo)
                        val userInfoKey: String = "user_info:" + userInfo.id
                        jedisClient.set(userInfoKey, userInfoJson)

                    }
                    jedisClient.close()
                })
            }
        }

        ssc.start()
        ssc.awaitTermination()

    }

}
