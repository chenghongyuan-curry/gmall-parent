package com.atguigu.realtime.service

import com.alibaba.fastjson.JSON
import com.atguigu.common.constants.GmallConstants
import com.atguigu.realtime.bean.OrderInfo
import com.atguigu.realtime.util.MykafkaUtil
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author chy
  * @date 2019-10-22 19:40
  */
object OrderApp {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("OrderApp").setMaster("local[*]")
        val ssc: StreamingContext = new StreamingContext(conf, Seconds(5))

        val inputDStream: InputDStream[ConsumerRecord[String, String]] = MykafkaUtil.getKafkaStream(GmallConstants.KAFKA_ORDER, ssc)

        val valueDStream: DStream[String] = inputDStream.map(_.value())
        valueDStream.print()

        val orderInfoDStream: DStream[OrderInfo] = valueDStream.map {
            orderJson => {
                val orderInfo: OrderInfo = JSON.parseObject(orderJson, classOf[OrderInfo])
                val timeArr: Array[String] = orderInfo.create_time.split(" ")

                orderInfo.create_date = timeArr(0)
                orderInfo.create_hour = timeArr(1).split(":")(0)

                //脱敏 13834561234=>138****1234
                val telTuple: (String, String) = orderInfo.consignee_tel.splitAt(3)
                val front3: String = telTuple._1
                val last4: String = telTuple._2.splitAt(4)._2
                orderInfo.consignee_tel = front3 + "****" + last4
                orderInfo
            }
        }

        import org.apache.phoenix.spark._

        orderInfoDStream.foreachRDD{rdd=>
            rdd.saveToPhoenix("GMALL_ORDER_INFO", Seq("ID","PROVINCE_ID", "CONSIGNEE", "ORDER_COMMENT", "CONSIGNEE_TEL", "ORDER_STATUS", "PAYMENT_WAY", "USER_ID","IMG_URL", "TOTAL_AMOUNT", "EXPIRE_TIME", "DELIVERY_ADDRESS", "CREATE_TIME","OPERATE_TIME","TRACKING_NO","PARENT_ORDER_ID","OUT_TRADE_NO", "TRADE_BODY", "CREATE_DATE", "CREATE_HOUR"), new Configuration(), Some("hadoop102,hadoop103,hadoop104:2181"))
        }


        ssc.start()
        ssc.awaitTermination()
    }

}
