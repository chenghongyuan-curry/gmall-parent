package com.atguigu.realtime.util

import java.io.InputStreamReader
import java.util.Properties

/**
  * @author chy
  * @date 2019-10-19 12:34
  */
object PropertiesUtil {
    def main(args: Array[String]): Unit = {
        val properties: Properties = PropertiesUtil.load("config.properties")

        println(properties.getProperty("kafka.broker.list"))
    }

    def load(propertieName: String): Properties = {
        val prop = new Properties();
        prop.load(new InputStreamReader(Thread.currentThread().getContextClassLoader.getResourceAsStream(propertieName), "UTF-8"))
        prop
    }

}
