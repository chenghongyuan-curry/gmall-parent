package com.atguigu.gmallpublisher.mapper;

import java.util.List;
import java.util.Map;

/**
 * @author chy
 * @date 2019-10-22 16:57
 */
public interface OrderMapper {

    public Double getOrderAmount(String date);

    public List<Map> getOrderAmountByHour(String date);


}
