package com.mrxc.mallpublisher.service;

import java.util.HashMap;

public interface SaleDetailService {

    public HashMap<String, Object> getSaleDetail(String date, Integer startpage, Integer size, String keyword);

}
