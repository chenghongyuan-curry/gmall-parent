package com.atguigu.realtime.bean

/**
  * @author chy
  * @date 2019-10-25 19:09
  */
case class EventInfo(mid:String,
                uid:String,
                appid:String,
                area:String,
                os:String,
                ch:String,
                `type`:String,
                evid:String ,
                pgid:String ,
                npgid:String ,
                itemid:String,
                var logDate:String,
                var logHour:String,
                var ts:Long
               ) {

}
