# gmall-parent
day01
1、搭建平台
2、实时数仓模拟产生数据
3、kfka采集数据

day02
1、消费kafka
2、数据流 转换 结构变成case class 补充两个时间字段
3、利用用户清单进行过滤 去重  只保留清单中不存在的用户访问记录
4、批次内进行去重：：按照mid 进行分组，每组取第一个值
5、保存今日访问过的用户(mid)清单   -->Redis    1 key类型 ： set    2 key ： dau:2019-xx-xx   3 value : mid

day03
1、通过Hbase的可视化软件Phoenix将数据存到Hbase
2、搭建SpringBoot,处理前端发送的请求，发布接口
3、通过已有的dw-chart对数据进行实时展示


day04
1、添加新功能：预警信息提示
2、预警信息要求：
a 三次几以上领取优惠券->在组内根据mid的事件集合进行 过滤筛选
b 用不同账号
c 在过程中没有浏览商品
同时满足以上三个条件，才可以发送预警信息
3、使用kibana得到商品预警信息top10树状图

day05
1、跟踪mysql数据库的数据，合成宽表，放进es中
2、编写es查询语句
3、使用springboot发布接口，加入相应查询语句
