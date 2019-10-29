package com.atguigu.gmallpublisher.service;

import java.util.Map;

/**
 * @author chy
 * @date 2019-10-21 16:49
 */
public interface PublisherService {

    public Long getDauTotal(String date);

    public Map getDauByHourTotal(String date);

    public Double getOrderAmount(String date);

    public Map getOrderAmountByHour(String date);

    public Map getSaleDatail(String date,int page,int size,String keyword);

}
