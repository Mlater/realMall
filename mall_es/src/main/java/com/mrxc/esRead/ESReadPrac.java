package com.mrxc.esRead;

import com.alibaba.fastjson.JSONObject;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.search.aggregation.MetricAggregation;
import io.searchbox.core.search.aggregation.TermsAggregation;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.support.ValueType;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class ESReadPrac {
    public static void main(String[] args) throws IOException {
        //创建连接
        JestClientFactory clientFactory = new JestClientFactory();

        HttpClientConfig httpClientConfig = new HttpClientConfig.Builder("http://master102:9200").build();
        clientFactory.setHttpClientConfig(httpClientConfig);

        JestClient client = clientFactory.getObject();

        //创建查询语句的对象
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        boolQueryBuilder.filter(new TermQueryBuilder("sex", "male"));
        boolQueryBuilder.must(new MatchQueryBuilder("favo","篮球"));
        //具体的过滤查询
        searchSourceBuilder.query(boolQueryBuilder);

        TermsAggregationBuilder count_by_sex = new TermsAggregationBuilder("group_sex", ValueType.LONG);
        count_by_sex.field("sex");
        count_by_sex.size(2);
        searchSourceBuilder.aggregation(count_by_sex);

        TermsAggregationBuilder group_name = new TermsAggregationBuilder("group_name", ValueType.LONG);
        group_name.field("name");
        group_name.size(10);
        searchSourceBuilder.aggregation(group_name);

        Search search = new Search.Builder(searchSourceBuilder.toString()).build();
        System.out.println(search.getIndex());

        //具体的执行，参数为Action类型，实现为Search对象，最后得到的是结果对象
        SearchResult result = client.execute(search);

        //解析result
        System.out.println("共筛选数据条数："  + result.getTotal());
        System.out.println("最高分："  + result.getMaxScore());

        //hits 标签中是json数组
        List<SearchResult.Hit<Map, Void>> hits = result.getHits(Map.class);
        for (SearchResult.Hit<Map, Void> hit : hits) {
            //封装hits中的单个json,封装我们真正关心的数据
            JSONObject jsonObject = new JSONObject();
            //获取到真正的数据
            Map source = hit.source;
            //遍历Map,把值赋值到json中
            for (Object key : source.keySet()) {
                jsonObject.put((String) key, source.get(key));
            }
            jsonObject.put("index", hit.index);
            jsonObject.put("type",hit.type);
            jsonObject.put("id", hit.id);
            jsonObject.put("score", hit.score);
            System.out.println(jsonObject.toString());
        }

        //解析聚合组数据
        MetricAggregation aggregations = result.getAggregations();

        TermsAggregation group_name1 = aggregations.getTermsAggregation("group_name");
        for (TermsAggregation.Entry bucket : group_name1.getBuckets()) {
            System.out.println(bucket.getKey() + "->" + bucket.getCount());
        }
        TermsAggregation group_sex = aggregations.getTermsAggregation("group_sex");
        for (TermsAggregation.Entry bucket : group_sex.getBuckets()) {
            System.out.println(bucket.getKey() + "->" + bucket.getCount());
        }

        client.shutdownClient();
    }
}
