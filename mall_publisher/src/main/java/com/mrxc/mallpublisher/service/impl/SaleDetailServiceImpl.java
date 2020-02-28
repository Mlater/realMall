package com.mrxc.mallpublisher.service.impl;

import com.mrxc.constants.MallConstants;
import com.mrxc.mallpublisher.service.SaleDetailService;
import io.searchbox.client.JestClient;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.search.aggregation.MetricAggregation;
import io.searchbox.core.search.aggregation.TermsAggregation;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
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

@Service
public class SaleDetailServiceImpl implements SaleDetailService {
    @Autowired
    JestClient jestClient;

    @Override
    public HashMap<String, Object> getSaleDetail(String date, Integer startpage, Integer size, String keyword) {

        //查询语句对象
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        //创建过滤的对象
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        //1、过滤日期的操作
        boolQueryBuilder.filter(new TermQueryBuilder("dt", date));
        //匹配关键词，设置分词的匹配操作为and
        MatchQueryBuilder sku_name = new MatchQueryBuilder("sku_name", keyword);
        sku_name.operator(MatchQueryBuilder.Operator.AND);
        //2、过滤关键词的操作
        boolQueryBuilder.must(sku_name);

        //执行查找操作
        searchSourceBuilder.query(boolQueryBuilder);

        //1、创建分组聚合对象
        TermsBuilder group_by_sex_Terms = AggregationBuilders.terms("group_by_sex").field("user_gender").size(2);
        TermsBuilder group_by_age_Terms = AggregationBuilders.terms("group_by_age").field("user_age").size(100);
        //执行分组聚合操作
        searchSourceBuilder.aggregation(group_by_sex_Terms);
        searchSourceBuilder.aggregation(group_by_age_Terms);

        //执行分页操作
        searchSourceBuilder.from((startpage - 1) * size);
        searchSourceBuilder.size(size);

        //创建搜索对象，指定Index和type
        Search search = new Search.Builder(searchSourceBuilder.toString())
                .addIndex(MallConstants.Mall_SALE_DETAIL_INDEX)
                .addType("_doc")
                .build();

        //先创建结果对象为null
        SearchResult result = null;
        //1、执行搜索，根据参数类型创建上面的对象
        try {
            result = jestClient.execute(search);
        } catch (IOException e) {
            e.printStackTrace();
        }


        //2、对结果进行解析
        //定义一个结果对象
        assert result != null;

        //我们把结果封装为map类型，有total总数，详细数据detail,还有两个聚合的结果
        HashMap<String, Object> resultMap = new HashMap<>();
        //定义详细数据detail的类型
        ArrayList<Map> detailMaps = new ArrayList<>();
        //定义按照性别分组的结果类型
        HashMap<String, Long> sexMap = new HashMap<>();
        //定义按照年龄分组的结果类型
        HashMap<String, Long> ageMap = new HashMap<>();

        //得到结果的个数
        Long total = result.getTotal();

        System.out.println(total);

        //封装详细数据detail
        List<SearchResult.Hit<Map, Void>> hits = result.getHits(Map.class);
        for (SearchResult.Hit<Map, Void> hit : hits) {
            detailMaps.add(hit.source);
        }

        //解析聚合组的数据
        MetricAggregation aggregations = result.getAggregations();
        //封装按照性别分组的结果
        TermsAggregation group_by_sex = aggregations.getTermsAggregation("group_by_sex");
        for (TermsAggregation.Entry bucket : group_by_sex.getBuckets()) {
            sexMap.put(bucket.getKey(), bucket.getCount());
        }

        //封装按照年龄分组的结果
        TermsAggregation group_by_age = aggregations.getTermsAggregation("group_by_age");
        for (TermsAggregation.Entry bucket : group_by_age.getBuckets()) {
            ageMap.put(bucket.getKey(), bucket.getCount());
        }

        //封装最后的结果
        resultMap.put("total", total);
        resultMap.put("detail", detailMaps);
        resultMap.put("sexMap", sexMap);
        resultMap.put("ageMap", ageMap);

        return resultMap;
    }
}
