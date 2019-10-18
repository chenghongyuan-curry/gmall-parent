package com.atguigu.gmalllogger.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.common.constants.GmallConstants;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

/**
 * @author chy
 * @date 2019-10-18 14:02
 */
@RestController
@Slf4j
public class LogJsonController {

    @Autowired
    KafkaTemplate<String,String> kafkaTemplate;


    @PostMapping("/log")
    public String doLog(@RequestParam("logString") String logString) {

        //补充时间戳
        JSONObject jsonObject = JSON.parseObject(logString);
        jsonObject.put("ts", System.currentTimeMillis());

        //落盘
        String jsonString = jsonObject.toJSONString();
        log.info(logString);

        //推送到kafka
        if("startup".equals(jsonObject.getString("type"))){
            kafkaTemplate.send(GmallConstants.KAFKA_TOPIC_STARTUP,jsonString);
        }else{
            kafkaTemplate.send(GmallConstants.KAFKA_TOPIC_EVENT,jsonString);
        }


        return "success";
    }

}