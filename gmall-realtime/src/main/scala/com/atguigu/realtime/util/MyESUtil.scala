package com.atguigu.realtime.util

import java.util

import io.searchbox.client.config.HttpClientConfig
import io.searchbox.client.{JestClient, JestClientFactory}
import io.searchbox.core.{Bulk, BulkResult, Index}

/**
  * @author chy
  * @date 2019-10-25 20:12
  */
object MyESUtil {
    private val ES_HOST = "http://hadoop102"
    private val ES_HTTP_PORT = 9200
    private var factory: JestClientFactory = null

    /**
      * 获取客户端
      *
      * @return jestclient
      */
    def getClient: JestClient = {
        if (factory == null) build()
        factory.getObject
    }

    /**
      * 关闭客户端
      */
    def close(client: JestClient): Unit = {
        if (client != null) try
            client.shutdownClient()
        catch {
            case e: Exception =>
                e.printStackTrace()
        }
    }

    /**
      * 建立连接
      */
    private def build(): Unit = {
        factory = new JestClientFactory
        factory.setHttpClientConfig(new HttpClientConfig.Builder(ES_HOST + ":" + ES_HTTP_PORT).multiThreaded(true)
                .maxTotalConnection(20) //连接总数
                .connTimeout(10000).readTimeout(10000).build)

    }

    // 批量插入数据到ES
    def insertBulk(souceList: List[(String, Any)],indexName:String,typeName:String) = {
        val jestclient: JestClient = getClient
        val builder: Bulk.Builder = new Bulk.Builder()
        for (source <- souceList) {
            val index: Index = new Index.Builder(source).index(indexName).`type`(typeName).build()
            builder.addAction(index)
        }

        val bulk: Bulk = builder.build()

        val items: util.List[BulkResult#BulkResultItem] = jestclient.execute(bulk).getItems
        println("保存" + items.size() + "条数据")
    }

}
