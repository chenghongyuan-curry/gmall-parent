package com.atguigu.gmallpublisher.service.impl;

import com.atguigu.common.constants.GmallConstants;
import com.atguigu.gmallpublisher.mapper.DauMapper;
import com.atguigu.gmallpublisher.mapper.OrderMapper;
import com.atguigu.gmallpublisher.service.PublisherService;
import io.searchbox.client.JestClient;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.search.aggregation.TermsAggregation;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
/**
 * @author chy
 * @date 2019-10-21 16:49
 */
@Service
public class PublisherServiceImpl implements PublisherService {

    @Autowired
    DauMapper dauMapper;

    @Autowired
    OrderMapper orderMapper;

    @Autowired
    JestClient jestClient;

    @Override
    public Long getDauTotal(String date) {
        return dauMapper.getDauTotal(date);
    }

    @Override
    public Map getDauByHourTotal(String date) {
        HashMap<String, Long> hashMap = new HashMap();

        List<Map> dauByHourTotal = dauMapper.getDauByHourTotal(date);
        for (Map map : dauByHourTotal) {
            String lh = (String) map.get("LH");
            Long ct = (Long) map.get("CT");
            hashMap.put(lh, ct);
        }
        return hashMap;

    }

    @Override
    public Double getOrderAmount(String date) {
        return orderMapper.getOrderAmount(date);
    }

    @Override
    public Map getOrderAmountByHour(String date) {
        HashMap<String, Double> hashMap = new HashMap();

        List<Map> orderAmountByHour = orderMapper.getOrderAmountByHour(date);
        for (Map map : orderAmountByHour) {
            String ch = (String) map.get("CHOUR");
            Double oa = (Double) map.get("OA");
            hashMap.put(ch, oa);
        }
        return hashMap;
    }

    @Override
    public Map getSaleDatail(String date, int page, int size, String keyword) {
        //查询语句
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        //查询过滤
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        boolQueryBuilder.must(new MatchQueryBuilder("sku_name",keyword).operator(MatchQueryBuilder.Operator.AND));
        boolQueryBuilder.filter(new TermQueryBuilder("dt",date));

        searchSourceBuilder.query(boolQueryBuilder);

        //聚合
        TermsBuilder aggGender = AggregationBuilders.terms("groupby_gender").field("user_gender").size(2);
        TermsBuilder aggAge = AggregationBuilders.terms("groupby_age").field("user_age").size(120);

        searchSourceBuilder.aggregation(aggGender);
        searchSourceBuilder.aggregation(aggAge);

        //分页
        searchSourceBuilder.from((page-1)*size);
        searchSourceBuilder.size(size);

        System.out.println(searchSourceBuilder.toString());

        //执行查询
        Search search = new Search.Builder(searchSourceBuilder.toString()).addIndex(GmallConstants.ES_INDEX_SALE_DETAIL).addType(GmallConstants.ES_TYPE_DEFAULT).build();
        HashMap resultMap = new HashMap<>();

        //明细list
        try {
            ArrayList<Map> saleDetailList = new ArrayList<>();
            SearchResult searchResult = jestClient.execute(search);
            List<SearchResult.Hit<Map, Void>> hits = searchResult.getHits(Map.class);
            for (SearchResult.Hit<Map, Void> hit : hits) {
                saleDetailList.add(hit.source);
            }

            //性别聚合结构
            List<TermsAggregation.Entry> gender_buckets = searchResult.getAggregations().getTermsAggregation("groupby_gender").getBuckets();
            HashMap genderMap = new HashMap();
            for (TermsAggregation.Entry bucket : gender_buckets) {
                genderMap.put( bucket.getKey(),bucket.getCount());
            }

            //年龄聚合结构
            List<TermsAggregation.Entry> age_buckets = searchResult.getAggregations().getTermsAggregation("groupby_age").getBuckets();
            HashMap ageMap = new HashMap();
            for (TermsAggregation.Entry bucket : age_buckets) {
                ageMap.put( bucket.getKey(),bucket.getCount());
            }

            //查询结果总数
            Long total = searchResult.getTotal();

            resultMap.put("saleDetailList",saleDetailList );
            resultMap.put("genderMap",genderMap );
            resultMap.put("ageMap",ageMap );
            resultMap.put("total",total );



        } catch (IOException e) {
            e.printStackTrace();
        }


        return resultMap;
    }


}
