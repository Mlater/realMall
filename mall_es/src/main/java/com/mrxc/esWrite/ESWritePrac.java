package com.mrxc.esWrite;

import com.mrxc.bean.person;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Index;

import java.io.IOException;

public class ESWritePrac {
    public static void main(String[] args) throws IOException {
        //首先要创建连接
        JestClientFactory jestClientFactory = new JestClientFactory();

        //设置参数
        HttpClientConfig httpClientConfig = new HttpClientConfig.Builder("http://master102:9200").build();
        jestClientFactory.setHttpClientConfig(httpClientConfig);

        JestClient client = jestClientFactory.getObject();

        person person1 = new person();
        person1.setId(001);
        person1.setName("lisi");
        person1.setSex("male");
        person1.setFavo("喝酒");

        Index index = new Index.Builder(person1)
                .index("stu")
                .type("_doc")
                .id("1003")
                .build();
        //执行的参数需要一个Action对象，其实质为写入的语句
        client.execute(index);


        client.shutdownClient();
    }
}
