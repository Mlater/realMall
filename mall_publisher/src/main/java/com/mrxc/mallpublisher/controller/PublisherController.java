package com.mrxc.mallpublisher.controller;

import com.alibaba.fastjson.JSON;
import com.mrxc.mallpublisher.bean.Option;
import com.mrxc.mallpublisher.bean.stat;
import com.mrxc.mallpublisher.service.impl.DAUServiceImpl;
import com.mrxc.mallpublisher.service.impl.GMVServiceImpl;
import com.mrxc.mallpublisher.service.impl.SaleDetailServiceImpl;
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
    DAUServiceImpl dauServiceImpl;

    @Autowired
    GMVServiceImpl gmvServiceImpl;

    @Autowired
    SaleDetailServiceImpl saleDetailServiceImpl;

    //对服务层返回的数据做包装
    //总数	http://localhost:8080/realtime-total?date=2019-09-06
    @GetMapping("realtime-total")
    public String getRealTimeTotal(@RequestParam("date") String date){

        //接收服务层获取的数据
        int total = dauServiceImpl.getTotal(date);
        Double amount = gmvServiceImpl.getOrderAmountTotal(date);


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
            todayData = dauServiceImpl.getRealTimeData(date);
            yesterdayData = dauServiceImpl.getRealTimeData(yesterday);
        } else if("order_amount".equals(id)){
            todayData = gmvServiceImpl.getOrderAmountHourMap(date);
            yesterdayData = gmvServiceImpl.getOrderAmountHourMap(yesterday);
        }

        resultHashMap.put("today", todayData);
        resultHashMap.put("yesterday", yesterdayData);

        return  JSON.toJSONString(resultHashMap);
    }

    @GetMapping("sale_detail")
    public String getSaleDetail(@RequestParam("date") String date,@RequestParam("startpage") Integer startpage,
                                @RequestParam("size") Integer size,@RequestParam("keyword") String keyword){
        //从sevice服务中返回的数据并没有比例，在这里进行数据展示的包装
        //获取sevice服务中返回的数据
        HashMap<String, Object> saleDetail = saleDetailServiceImpl.getSaleDetail(date, startpage, size, keyword);

        //用来封装最后输出的结果
        HashMap<String, Object> result = new HashMap<>();
        //得到总数
        Long total = (Long)saleDetail.get("total");
        //得到详情
        List detail = (List)saleDetail.get("detail");

        //得到性别详情
        Map sexMap = (Map)saleDetail.get("sexMap");
        //得到男性的count
        Long femaleCount = (Long)sexMap.get("F");
        //计算性别比例，这里的百分比展示时自己处理，考虑四舍五入，比如50.0%，此处的结果为50.0
        double femaleRatio = Math.round(femaleCount * 1000 / total)/ 10D;
        double maleRatio = 100D - femaleRatio;

        //封装为对象
        Option maleOption = new Option("男", femaleRatio);
        Option femaleOption = new Option("女", maleRatio);

        ArrayList<Option> sexList = new ArrayList<>();
        sexList.add(maleOption);
        sexList.add(femaleOption);

        stat sexStat = new stat(sexList, "用户性别占比");

        //得到年龄详情
        Map ageMap = (Map)saleDetail.get("ageMap");

        Long Lower20 = 0L;
        Long start20To30 = 0L;
        //30岁以上的比例直接用100减去30岁以下的就可以了，不需要单独计算
        //Long start30 = 0L;

        //遍历年龄的种类
        for (Object age : ageMap.keySet()) {
            Integer ageKey = Integer.valueOf(age.toString());
            //获取每个年龄的人数
            //Long ageCount = ageMap.get(ageKey) == null? 0L:(Long) ageMap.get(ageKey);
            Long ageCount = (Long)ageMap.get(age);

            if (ageKey < 20) {
                Lower20 += ageCount;
            } else if (ageKey < 30) {
                start20To30 += ageCount;
            }
        }

            double lower20Ratio = Math.round(Lower20 * 1000 /total) /10D;
            double start20To30Ratio = Math.round(start20To30 * 1000 /total) /10D;
            double upper30Ratio = 100D - lower20Ratio - start20To30Ratio;

            Option lower20Option = new Option("20岁以下", lower20Ratio);
            Option start20To30Option = new Option("20岁到30岁", start20To30Ratio);
            Option upper30Option = new Option("30岁以上", upper30Ratio);

            ArrayList<Option> ageList = new ArrayList<>();
            ageList.add(lower20Option);
            ageList.add(start20To30Option);
            ageList.add(upper30Option);

            stat ageStat = new stat(ageList, "用户年龄占比");

            //创建stat对象的集合
            ArrayList<stat> stats = new ArrayList<>();

            stats.add(ageStat);
            stats.add(sexStat);

            result.put("total",total);
            result.put("stat",stats);
            result.put("detail",detail);

            return JSON.toJSONString(result);
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
