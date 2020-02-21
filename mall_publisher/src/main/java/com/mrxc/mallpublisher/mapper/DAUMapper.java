package com.mrxc.mallpublisher.mapper;

import java.util.List;
import java.util.Map;

public interface DAUMapper {
    //获取总数
    //总数	[{"id":"dau","name":"新增日活","value":1200},
    //{"id":"new_mid","name":"新增设备","value":233}]
    public int getTotal(String date);

    //获取分时统计的数据
    public List<Map> getRealTimeData(String date);
}
