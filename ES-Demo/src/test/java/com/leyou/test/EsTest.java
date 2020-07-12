package com.leyou.test;

import com.leyou.entity.Item;
import com.leyou.es.repository.ItemRepository;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.sort.ScoreSortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.data.elasticsearch.core.ElasticsearchTemplate;
import org.springframework.data.elasticsearch.core.aggregation.AggregatedPage;
import org.springframework.data.elasticsearch.core.query.FetchSourceFilter;
import org.springframework.data.elasticsearch.core.query.NativeSearchQueryBuilder;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.ArrayList;
import java.util.List;

/**
 * @author zycstart
 * @create 2020-07-11 20:53
 */
@RunWith(SpringRunner.class)
@SpringBootTest
public class EsTest {
    // 复杂操作，例如聚合的时候用，简单的CRUD会使用ElasticsearchCrudRepository，类似于通用mapper
    @Autowired
    ElasticsearchTemplate template;
    @Autowired
    ItemRepository repository;

    @Test
    public void testCreateIndex() {
        // 创建索引库，会根据Item类的@Document注解信息来创建
        template.createIndex(Item.class);
        // 映射关系，会根据Item类中的id、Field等字段来自动完成映射
        template.putMapping(Item.class);
    }

    @Test
    public void deleteIndex() {
        template.deleteIndex("heima2");
    }

    /**
     * 新增文档
     */
    @Test
    public void index() {
        Item item = new Item(1L, "小米手机7", " 手机",
                "小米", 3499.00, "http://image.leyou.com/13123.jpg");
        repository.save(item);
    }

    @Test
    public void indexList() {
        List<Item> list = new ArrayList<>();
        list.add(new Item(2L, "坚果手机R1", " 手机", "锤子", 3699.00, "http://image.leyou.com/123.jpg"));
        list.add(new Item(3L, "华为META10", " 手机", "华为", 4499.00, "http://image.leyou.com/3.jpg"));
        // 接收对象集合，实现批量新增
        repository.saveAll(list);
    }

    @Test
    public void testFind() {
        // 查询全部，并安装价格降序排序
        Iterable<Item> items = repository.findAll(Sort.by(Sort.Direction.DESC, "price"));
        items.forEach(System.out::println);
    }

    @Test
    public void testFindBy() {
        // 自定义查询
        List<Item> items = repository.findByPriceBetween(2000.00, 4000.00);
        items.forEach(System.out::println);
    }

    @Test
    public void testQuery() {
        MatchQueryBuilder queryBuilder = QueryBuilders.matchQuery("title", "小米手机");
        final Iterable<Item> search = repository.search(queryBuilder);
    }

    @Test
    public void testQuery1() {
        //  原生查询构建器
        NativeSearchQueryBuilder queryBuilder = new NativeSearchQueryBuilder();
        //  结果过滤
        queryBuilder.withSourceFilter(new FetchSourceFilter(new String[]{"id", "title", "price"}, null));
        //  添加查询条件
        queryBuilder.withQuery(QueryBuilders.matchQuery("title", "小米手机"));
        //  排序
        queryBuilder.withSort(SortBuilders.fieldSort("price").order(SortOrder.DESC));
        //  分页,页码是从0开始
        queryBuilder.withPageable(PageRequest.of(0, 2));
        Page<Item> result = repository.search(queryBuilder.build());

        long totalElements = result.getTotalElements();
        System.out.println("totalElements" + totalElements);
        int totalPages = result.getTotalPages();
        System.out.println("totalPages" + totalPages);
        List<Item> content = result.getContent();
        content.forEach(System.out::println);
    }

    @Test
    public void testAgg(){
        NativeSearchQueryBuilder queryBuilder = new NativeSearchQueryBuilder();
        //  聚合,可重复聚合
        queryBuilder.addAggregation(AggregationBuilders.terms("popularBrand").field("brand"));
        //  查询并返回带聚合的结果集
        AggregatedPage<Item> items = template.queryForPage(queryBuilder.build(), Item.class);
        //  解析聚合结果
        Aggregations itemsAggregations = items.getAggregations();
        //  获取指定名称的聚合
        StringTerms popularBrand = itemsAggregations.get("popularBrand");
        // 获取桶
        List<StringTerms.Bucket> buckets = popularBrand.getBuckets();
        for (StringTerms.Bucket bucket : buckets) {
            System.out.println(bucket.getKey());
            System.out.println(bucket.getDocCount());
        }
    }
}