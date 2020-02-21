package com.mrxc.malllogger.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.mrxc.constants.MallConstants;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@Slf4j
public class loggerControllerTest {

    @Autowired
    private KafkaTemplate<String,String> kafkaTemplate;

    @PostMapping("log")
    public String logger(@RequestParam("logString") String logStr){
        //System.out.println("logger");

        JSONObject jsonObject = JSON.parseObject(logStr);

        //添加时间字段
        jsonObject.put("ts",System.currentTimeMillis());

        String tsJson = jsonObject.toString();

        //使用log4j把日志打印到控制台和指定的文件中
        log.info(tsJson);

        //把数据发送到kafka集群
        if (tsJson.contains("startup")) {
            kafkaTemplate.send(MallConstants.Mall_STARTUP_TOPIC, tsJson);
        }else{
            kafkaTemplate.send(MallConstants.Mall_EVENT_TOPIC, tsJson);
        }

        return "success";
    }

    @GetMapping("log")
    public String test(@RequestParam("logaaa") String logStr){
        System.out.println(logStr);

        return "success";
    }


}
