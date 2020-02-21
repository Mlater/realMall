package com.mrxc.mallpublisher.service.impl;

import com.mrxc.mallpublisher.mapper.GMVMapper;
import com.mrxc.mallpublisher.service.GMVService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class GMVServiceImpl implements GMVService {

    @Autowired
    GMVMapper gmvMapper;

    @Override
    public Double getOrderAmountTotal(String date) {
        return gmvMapper.getOrderAmountTotal(date);
    }


    //加工数据，把mapper层返回的数据加工为map结构
    //旧数据：LH->11,ct->222,新数据：11->222
    @Override
    public Map getOrderAmountHourMap(String date) {
        List<Map> realTimeData = gmvMapper.getOrderAmountHourMap(date);

        HashMap<String, Double> map = new HashMap<String, Double>();

        for (Map realTimeDatum : realTimeData) {
            map.put((String) realTimeDatum.get("CREATE_HOUR"),(Double) realTimeDatum.get("AMOUNT_SUM"));
        }

        return map;
    }
}
