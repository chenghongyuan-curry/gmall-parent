package com.atguigu.gmallpublisher.bean;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.List;

/**
 * @author chy
 * @date 2019-10-29 19:53
 */
@Data
@AllArgsConstructor
public class Stat {

    String title;

    List<Option> options;
}

