package com.atguigu.mock.util;

/**
 * @author chy
 * @date 2019-10-18 12:30
 */
public class RanOpt<T> {
    T value ;
    int weight;

    public RanOpt ( T value, int weight ){
        this.value=value ;
        this.weight=weight;
    }

    public T getValue() {
        return value;
    }

    public int getWeight() {
        return weight;
    }

}
