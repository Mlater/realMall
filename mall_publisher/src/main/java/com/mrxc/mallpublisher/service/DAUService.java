package com.mrxc.mallpublisher.service;

import java.util.Map;

public interface DAUService {

    public int getTotal(String date);

    //获取分时统计的数据
    public Map getRealTimeData(String date);
}
