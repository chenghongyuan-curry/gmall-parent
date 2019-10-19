package com.atguigu.realtime.bean

/**
  * @author chy
  * @date 2019-10-19 12:35
  */
case class StartUpLog(mid:String,
                      uid:String,
                      appid:String,
                      area:String,
                      os:String,
                      ch:String,
                      logType:String,
                      vs:String,
                      var logDate:String,
                      var logHour:String,
                      var ts:Long
                     ) {

}

