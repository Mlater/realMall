package com.mrxc.mallpublisher;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@MapperScan(basePackages = "com.mrxc.mallpublisher.mapper")
public class MallPublisherApplication {

    public static void main(String[] args) {

        SpringApplication.run(MallPublisherApplication.class, args);
    }

}
