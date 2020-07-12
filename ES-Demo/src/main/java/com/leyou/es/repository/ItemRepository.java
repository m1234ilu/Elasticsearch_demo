package com.leyou.es.repository;

import com.leyou.entity.Item;
import org.springframework.data.elasticsearch.repository.ElasticsearchRepository;

import java.util.List;

/**
 * @author zycstart
 * @create 2020-07-11 21:09
 */
public interface ItemRepository extends ElasticsearchRepository<Item,Long> {

    List<Item> findByPriceBetween(Double begin, Double end);
}
