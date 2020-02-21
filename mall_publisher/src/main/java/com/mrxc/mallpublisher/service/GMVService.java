package com.mrxc.mallpublisher.service;

import org.springframework.stereotype.Service;

import java.util.Map;

public interface GMVService {

    public Double getOrderAmountTotal(String date);

    //获取分时统计的数据
    public Map getOrderAmountHourMap(String date);
}
