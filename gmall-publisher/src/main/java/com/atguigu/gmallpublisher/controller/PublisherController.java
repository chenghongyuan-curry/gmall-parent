package com.atguigu.gmallpublisher.controller;

import com.alibaba.fastjson.JSON;
import com.atguigu.gmallpublisher.bean.Option;
import com.atguigu.gmallpublisher.bean.Stat;
import com.atguigu.gmallpublisher.service.PublisherService;
import org.apache.commons.lang.time.DateUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * @author chy
 * @date 2019-10-21 18:17
 */
@RestController
public class PublisherController {

    @Autowired
    PublisherService publicService;



    @GetMapping("realtime-total")
    public String realtimeTotal(@RequestParam("date") String dateString){

        Long dauTotal = publicService.getDauTotal(dateString);

        List<Map> dauList = new ArrayList<Map>();
        Map dauMap = new HashMap<String,String>();

        dauMap.put("id","dau" );
        dauMap.put("name","新增日活" );
        dauMap.put("value",dauTotal );
        dauList.add(dauMap);

        Map midMap = new HashMap<String,String>();

        midMap.put("id","new_mid" );
        midMap.put("name","新增设备" );
        midMap.put("value",333 );
        dauList.add(midMap);

        Map orderMap = new HashMap();
        Double orderAmount = publicService.getOrderAmount(dateString);

        orderMap.put("id","order_amount" );
        orderMap.put("name","新增交易额" );
        orderMap.put("value",orderAmount);
        dauList.add(orderMap);


        return JSON.toJSONString(dauList);

    }

    @GetMapping("realtime-hour")
    public String realtimeHour(@RequestParam("id") String id,@RequestParam("date") String dateString){
        if("dau".equals(id)){
            Map hashMap = new HashMap<>();

            String yesterday = getYesterday(dateString);

            Map dauByHourTotalD = publicService.getDauByHourTotal(dateString);
            Map dauByHourTotal1YD = publicService.getDauByHourTotal(yesterday);

            hashMap.put("today", dauByHourTotalD);
            hashMap.put("yserterday", dauByHourTotal1YD);

            return JSON.toJSONString(hashMap);
        }else if("order_amount".equals(id)){
            Map hashMap = new HashMap<>();

            String yesterday = getYesterday(dateString);

            Map OrderAmountHourTotalD = publicService.getOrderAmountByHour(dateString);
            Map OrderAmountHourTotal1YD = publicService.getOrderAmountByHour(yesterday);

            hashMap.put("today", OrderAmountHourTotalD);
            hashMap.put("yserterday", OrderAmountHourTotal1YD);

            return JSON.toJSONString(hashMap);
        }

        return null;

    }

    @GetMapping("sale_detail")
    public String getSaleDetail(@RequestParam("date") String date,@RequestParam("startpage") int startpage,@RequestParam("size") int size,@RequestParam("keyword") String keyword){
        Map saleDatail = publicService.getSaleDatail(date, startpage, size, keyword);

        Long total = (Long)saleDatail.get("total");
        List saleDetailList = (List) saleDatail.get("saleDetailList");
        Map genderMap = (Map)saleDatail.get("genderMap");
        Map ageMap = (Map)saleDatail.get("ageMap");

        //  genderMap 整理成为  OptionGroup
        Long femaleCount =(Long) genderMap.get("F");
        Long maleCount =(Long) genderMap.get("M");
        double femaleRate = Math.round(femaleCount * 1000D / total) / 10D;
        double maleRate = Math.round(maleCount * 1000D / total) / 10D;
        List<Option> genderOptions=new ArrayList<>();
        genderOptions.add( new Option("男", maleRate));
        genderOptions.add( new Option("女", femaleRate));
        Stat genderOptionGroup = new Stat("性别占比", genderOptions);

        //  ageMap 整理成为  OptionGroup

        Long age_20Count=0L;
        Long age20_30Count=0L;
        Long age30_Count=0L;
        for (Object o : ageMap.entrySet()) {
            Map.Entry entry = (Map.Entry) o;
            String agekey =(String) entry.getKey();
            int age = Integer.parseInt(agekey);
            Long ageCount =(Long) entry.getValue();
            if(age <20){
                age_20Count+=ageCount;
            }else   if(age>=20&&age<30){
                age20_30Count+=ageCount;
            }else{
                age30_Count+=ageCount;
            }
        }
        Double age_20rate=0D;
        Double age20_30rate=0D;
        Double age30_rate=0D;

        age_20rate = Math.round(age_20Count * 1000D / total) / 10D;
        age20_30rate = Math.round(age20_30Count * 1000D / total) / 10D;
        age30_rate = Math.round(age30_Count * 1000D / total) / 10D;
        List<Option> ageOptions=new ArrayList<>();
        ageOptions.add( new Option("20岁以下",age_20rate));
        ageOptions.add( new Option("20岁到30岁",age20_30rate));
        ageOptions.add( new Option("30岁以上",age30_rate));
        Stat ageOptionGroup = new Stat("年龄占比", ageOptions);

        List<Stat> statGroupList=new ArrayList<>();
        statGroupList.add(genderOptionGroup);
        statGroupList.add(ageOptionGroup);

        HashMap resultMap = new HashMap();
        resultMap.put("total", total);
        resultMap.put("stat", statGroupList);
        resultMap.put("detail", saleDetailList);

        return JSON.toJSONString(resultMap);

    }

    public String getYesterday(String dateString){

      String yesterday = "";
        try {
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
            Date dateD = dateFormat.parse(dateString);
            Date dateYD = DateUtils.addDays(dateD, -1);
            yesterday = dateYD.toString();
        } catch (ParseException e) {
            e.printStackTrace();
        }

        return yesterday;
    }


}
