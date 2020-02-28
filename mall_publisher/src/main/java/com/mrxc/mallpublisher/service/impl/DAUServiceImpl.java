package com.mrxc.mallpublisher.service.impl;

import com.mrxc.mallpublisher.mapper.DAUMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class DAUServiceImpl{

    @Autowired
    DAUMapper dauMapper;


    public int getTotal(String date) {
        return dauMapper.getTotal(date);
    }

    //加工数据，把mapper层返回的数据加工为map结构
    //旧数据：LH->11,ct->222,新数据：11->222

    public Map getRealTimeData(String date) {
        List<Map> realTimeData = dauMapper.getRealTimeData(date);

        HashMap<String, Long> map = new HashMap<String, Long>();

        for (Map realTimeDatum : realTimeData) {
            map.put((String) realTimeDatum.get("LH"),(Long) realTimeDatum.get("CT"));
        }

        return map;
    }
}
