package com.atguigu.gmalllogger;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

@SpringBootApplication
public class GmallLoggerApplication {

    public static void main(String[] args) {
        SpringApplication.run(GmallLoggerApplication.class, args);
    }


}
