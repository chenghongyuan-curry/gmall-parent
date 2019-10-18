package com.atguigu.mock.util;

import java.util.Random;

/**
 * @author chy
 * @date 2019-10-18 12:31
 */
public class RandomNum {
    public static final  int getRandInt(int fromNum,int toNum){
        return   fromNum+ new Random().nextInt(toNum-fromNum+1);
    }

}
