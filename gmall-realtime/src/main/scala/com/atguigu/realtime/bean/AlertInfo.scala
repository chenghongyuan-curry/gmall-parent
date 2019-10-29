package com.atguigu.realtime.bean

/**
  * @author chy
  * @date 2019-10-25 19:08
  */
case class AlertInfo(mid:String,
                uids:java.util.HashSet[String],
                itemIds:java.util.HashSet[String],
                events:java.util.List[String],
                ts:Long)
{

}
