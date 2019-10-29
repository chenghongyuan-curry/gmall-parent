package com.atguigu.realtime.service

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.alibaba.fastjson.JSON
import com.atguigu.common.constants.GmallConstants
import com.atguigu.realtime.bean.{AlertInfo, EventInfo}
import com.atguigu.realtime.util.{MyESUtil, MykafkaUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.util.control.Breaks._

/**
  * @author chy
  * @date 2019-10-25 19:10
  */
object AlertApp {
    def main(args: Array[String]): Unit = {
        /*val conf: SparkConf = new SparkConf().setAppName("AlertApp").setMaster("local[*]")
        val ssc: StreamingContext = new StreamingContext(conf, Seconds(5))


        val inputDstream: InputDStream[ConsumerRecord[String, String]] = MykafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_EVENT, ssc)


        //转换成case class
        val eventInfoDstream: DStream[EventInfo] = inputDstream.map(record => {
            val jsonString: String = record.value()
            val eventInfo: EventInfo = JSON.parseObject(jsonString, classOf[EventInfo])

            val date: Date = new Date(eventInfo.ts)
            val dateformat: String = new SimpleDateFormat("yyyy-MM-dd HH").format(date)
            val dateStrings: Array[String] = dateformat.split(" ")
            eventInfo.logDate = dateStrings(0)
            eventInfo.logHour = dateStrings(1)

            eventInfo
        })

        val cacheDstream: DStream[EventInfo] = eventInfoDstream.cache()

        //开启滑动窗口，窗口大小5分钟，步长10s
        val eventInfoWindowDStream: DStream[EventInfo] = cacheDstream.window(Seconds(300), Seconds(10))

        //根据设备id分组
        val mapInfoKVDstream: DStream[(String, EventInfo)] = eventInfoWindowDStream.map(info => (info.mid, info))
        val infoGroupByKeyDstream: DStream[(String, Iterable[EventInfo])] = mapInfoKVDstream.groupByKey()


        //根据登录次数进行筛选
        val isAlertInfoDstream: DStream[(Boolean, AlertInfo)] = infoGroupByKeyDstream.map {
            case (mid, infoItr) => {

                val uidSet: util.HashSet[String] = new util.HashSet[String]()
                val itemIdSet: util.HashSet[String] = new util.HashSet[String]()
                val eventList: util.ArrayList[String] = new util.ArrayList[String]()
                //true发送报警 false不发送报警
                var isAlert = true
                breakable {
                    for (info <- infoItr) {
                        eventList.add(info.evid)
                        if (info.evid == "coupon") {
                            uidSet.add(info.uid)
                            itemIdSet.add(info.itemid)
                        }

                        if (info.evid == "clickItem") {
                            isAlert = false
                            break
                        }
                    }
                }

                if (uidSet.size() < 3) {
                    isAlert = false
                }

                (isAlert, AlertInfo(mid, uidSet, itemIdSet, eventList, System.currentTimeMillis()))

            }
        }


        //去重：isAlert=true的信息留下，发送报警信息
        val filterInfoDstream: DStream[(Boolean, AlertInfo)] = isAlertInfoDstream.filter(_._1)


        //保存到es中
        val midAndMidUnionKeysDstream: DStream[(String, AlertInfo)] = filterInfoDstream.map {
            case (isAlert, info) => {
                val t_min: Long = info.ts / 1000 / 60
                (info.mid + "_" + t_min, info)
            }
        }


        midAndMidUnionKeysDstream.foreachRDD(rdd => {
            rdd.foreachPartition { alertInfoItr => {
                MyESUtil.insertBulk(alertInfoItr.toList, GmallConstants.ES_INDEX, GmallConstants.ES_TYPE_DEFAULT)
            }
            }
        })



        ssc.start()
        ssc.awaitTermination()*/


        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("coupon_alert_app")

        val ssc= new StreamingContext(sparkConf,Seconds(5));

        val inputDstream: InputDStream[ConsumerRecord[String, String]] = MykafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_EVENT,ssc)

        //1 开窗


        //2 调整结构  => record => case class
        val eventDstream: DStream[EventInfo] = inputDstream.map { record =>
            val jsonString: String = record.value()
            val eventInfo: EventInfo = JSON.parseObject(jsonString, classOf[EventInfo])
            val formator = new SimpleDateFormat("yyyy-MM-dd HH")
            val dateHour: String = formator.format(new Date(eventInfo.ts))
            val dateHourArr: Array[String] = dateHour.split(" ")
            eventInfo.logDate = dateHourArr(0)
            eventInfo.logHour = dateHourArr(1)
            eventInfo
        }
        val eventWindowsDstream: DStream[EventInfo] = eventDstream.window(Seconds(300),Seconds(10))


        //3 分组
        val eventGroupbyMidDstream: DStream[(String, Iterable[EventInfo])] = eventDstream.map(eventInfo=>(eventInfo.mid,eventInfo)).groupByKey()

        //4 筛选
        //    a 三次几以上领取优惠券   //在组内根据mid的事件集合进行 过滤筛选
        //    b 用不同账号
        //    c  在过程中没有浏览商品

        // 1 判断出来是否达到预警的要求 2 如果达到要求组织预警的信息
        val  alertDstream: DStream[(Boolean, AlertInfo)] = eventGroupbyMidDstream.map { case (mid, eventInfoItr) =>
            var ifAlert = true; //
        // 登录过的uid
        val uidSet = new util.HashSet[String]()
            //  领取的商品Id
            val itemIdSet = new util.HashSet[String]()
            //  做过哪些行为
            val eventList = new util.ArrayList[String]()
            breakable(
                for (eventInfo: EventInfo <- eventInfoItr) {
                    if (eventInfo.evid == "coupon") {
                        uidSet.add(eventInfo.uid)
                        itemIdSet.add(eventInfo.itemid)
                    }
                    eventList.add(eventInfo.evid)
                    if (eventInfo.evid == "clickItem") {
                        ifAlert = false
                        break
                    }
                }
            )
            if (uidSet.size() < 3) { //超过3个及以上账号登录 符合预警要求
                ifAlert = false;
            }

            (ifAlert, AlertInfo(mid, uidSet, itemIdSet, eventList, System.currentTimeMillis()))
        }
        //过滤
        val filterAlertDstream: DStream[(Boolean, AlertInfo)] = alertDstream.filter(_._1 )
        //  转换结构(ifAlert,alerInfo) =>（ mid_minu  ,  alerInfo）
        val alertInfoWithIdDstream: DStream[(String, AlertInfo)] = filterAlertDstream.map { case (ifAlert, alertInfo) =>
            val uniKey = alertInfo.mid + "_" + alertInfo.ts / 1000 / 60
            (uniKey, alertInfo)
        }

        //5 存储->es   提前建好index  和 mapping
        alertInfoWithIdDstream.foreachRDD{rdd=>
            rdd.foreachPartition{alertInfoItr=>
                val alertList: List[(String, AlertInfo)] = alertInfoItr.toList
                MyESUtil.insertBulk(alertList ,GmallConstants.ES_INDEX,GmallConstants.ES_TYPE_DEFAULT )
            }

        }

        ssc.start()
        ssc.awaitTermination()
    }

}
