package com.atguigu.gmallpublisher.mapper;

import java.util.List;
import java.util.Map;

/**
 * @author chy
 * @date 2019-10-21 16:33
 */
public interface DauMapper {

    public Long getDauTotal(String date);

    public List<Map> getDauByHourTotal(String date);


}
