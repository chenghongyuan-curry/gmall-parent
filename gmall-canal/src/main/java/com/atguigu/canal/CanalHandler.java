package com.atguigu.canal;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.atguigu.canal.util.MykafkaSender;
import com.atguigu.common.constants.GmallConstants;

import java.util.List;

/**
 * @author chy
 * @date 2019-10-22 12:44
 */
public class CanalHandler {
    CanalEntry.EventType eventType;

    String tableName;

    List<CanalEntry.RowData> rowDataList;

    public CanalHandler(CanalEntry.EventType eventType, String tableName, List<CanalEntry.RowData> rowDataList) {
        this.eventType = eventType;
        this.tableName = tableName;
        this.rowDataList = rowDataList;
    }

    public void handle(){
        if("order_info".equals(tableName) && CanalEntry.EventType.INSERT == eventType){
            rowDateList2Kafka(GmallConstants.KAFKA_ORDER);
        }else if("user_info".equals(tableName) && CanalEntry.EventType.INSERT == eventType || CanalEntry.EventType.UPDATE==eventType){
            rowDateList2Kafka(GmallConstants.KAFKA_USER);
        }else if("order_detail".equals(tableName) && CanalEntry.EventType.INSERT == eventType){
            rowDateList2Kafka(GmallConstants.KAFKA_ORDER_DETAIL);
        }
    }


    private void  rowDateList2Kafka(String kafkaTopic){
        for (CanalEntry.RowData rowData : rowDataList) {
            List<CanalEntry.Column> afterColumnsList = rowData.getAfterColumnsList();
            JSONObject jsonObject = new JSONObject();

            for (CanalEntry.Column column : afterColumnsList) {
                String name = column.getName();
                String value = column.getValue();

                System.out.println(name + "::" +value);
                jsonObject.put(name, value);
            }

            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            MykafkaSender.send(kafkaTopic, jsonObject.toJSONString());
        }
    }

}
