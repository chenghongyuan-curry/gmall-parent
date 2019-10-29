package com.atguigu.realtime.test


/**
  * @author chy
  * @date 2019-10-20 11:57
  */
object TestPhoenix {
    def main(args: Array[String]): Unit = {
        import org.apache.spark.SparkContext
        import org.apache.phoenix.spark._

        val sc = new SparkContext("local", "phoenix-test")
        val dataSet = List((1L, "1", 1), (2L, "2", 2), (3L, "3", 3))

        sc.parallelize(dataSet).saveToPhoenix(
            "OUTPUT_TEST_TABLE",
            Seq("ID","COL1","COL2"),
            zkUrl = Some("hadoop102,hadoop103,hadoop104:2181")
        )




    }

}
