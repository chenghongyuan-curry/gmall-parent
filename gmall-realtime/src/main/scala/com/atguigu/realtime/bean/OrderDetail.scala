package com.atguigu.realtime.bean

/**
  * @author chy
  * @date 2019-10-27 12:40
  */
case class OrderDetail(
                              id:String ,
                              order_id: String,
                              sku_name: String,
                              sku_id: String,
                              order_price: String,
                              img_url: String,
                              sku_num: String
                      ) {

}
