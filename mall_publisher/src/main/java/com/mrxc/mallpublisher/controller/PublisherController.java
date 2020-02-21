package com.mrxc.mallpublisher.controller;

import com.alibaba.fastjson.JSON;
import com.mrxc.mallpublisher.service.DAUService;
import com.mrxc.mallpublisher.service.GMVService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

@RestController
public class PublisherController {

    @Autowired
    DAUService dauService;

    @Autowired
    GMVService gmvService;

    //对服务层返回的数据做包装
    //总数	http://localhost:8080/realtime-total?date=2019-09-06
    @GetMapping("realtime-total")
    public String getRealTimeTotal(@RequestParam("date") String date){

        //接收服务层获取的数据
        int total = dauService.getTotal(date);
        Double amount = gmvService.getOrderAmountTotal(date);


        //因为最后的输出格式为JSON数组格式，所以先创建集合,里面是kv类型，所以使用Map
        //[{"id":"dau","name":"新增日活","value":1200},
        //{"id":"new_mid","name":"新增设备","value":233 },
        //{"id":"order_amount","name":"新增交易额","value":1000.2 }]
        ArrayList<Map> totalResult = new ArrayList<Map>();

        //创建内部的新增日活map
        HashMap<String, Object> dauHashMap = new HashMap<String, Object>();
        dauHashMap.put("id","dau");
        dauHashMap.put("name","新增日活");
        dauHashMap.put("value",total);

        //创建内部的新增设备map
        HashMap<String, Object> newMidHashMap = new HashMap<String, Object>();
        newMidHashMap.put("id","new_mid");
        newMidHashMap.put("name","新增设备");
        newMidHashMap.put("value","9999");

        //创建内部的新增交易额map
        HashMap<String, Object> newPayCountHashMap = new HashMap<String, Object>();
        newPayCountHashMap.put("id","order_amount");
        newPayCountHashMap.put("name","新增交易额");
        newPayCountHashMap.put("value",amount);

        totalResult.add(dauHashMap);
        totalResult.add(newMidHashMap);
        totalResult.add(newPayCountHashMap);

        return JSON.toJSONString(totalResult);
}

    //分时统计	http://localhost:8080/realtime-hours?id=dau&date=2019-09-06
    @GetMapping("realtime-hours")
    public String getRealTimeDAU(@RequestParam("id") String id ,@RequestParam("date") String date){
        //{"yesterday":{"11":383,"12":123,"17":88,"19":200 },
        //"today":{"12":38,"13":1233,"17":123,"19":688 }}

        HashMap<String, Map> resultHashMap = new HashMap<>();
        //封装2天的数据
        Map todayData = null;
        Map yesterdayData = null;

        //获取到昨天的日期
        String yesterday = getYesterday(date);

        if ("dau".equals(id)) {
            todayData = dauService.getRealTimeData(date);
            yesterdayData = dauService.getRealTimeData(yesterday);
        } else if("order_amount".equals(id)){
            todayData = gmvService.getOrderAmountHourMap(date);
            yesterdayData = gmvService.getOrderAmountHourMap(yesterday);
        }

        resultHashMap.put("today", todayData);
        resultHashMap.put("yesterday", yesterdayData);

        return  JSON.toJSONString(resultHashMap);
    }

    private String getYesterday(@RequestParam("date") String date) {
        //结果要返回2天，所以要获取昨天的时间
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");

        Calendar dateInstance = Calendar.getInstance();
        try {
            dateInstance.setTime(simpleDateFormat.parse(date));
        } catch (ParseException e) {
            e.printStackTrace();
        }

        //求出昨天的时间
        dateInstance.add(Calendar.DAY_OF_MONTH,-1);

        return simpleDateFormat.format(new Date(dateInstance.getTimeInMillis()));
    }
}
