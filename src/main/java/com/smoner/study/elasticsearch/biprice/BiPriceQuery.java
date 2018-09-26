package com.smoner.study.elasticsearch.biprice;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.net.InetAddress;

/**
 * Created by fengjqc on 2018/9/26.
 */
public class BiPriceQuery {
    public static void main(String[] args) throws Exception{
        Settings settings = Settings.builder().put("cluster.name","elasticsearch").build();
        TransportClient transportClient = new PreBuiltTransportClient(settings).addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"),9300));


        SearchResponse response = transportClient.prepareSearch("yc-biprice").setTypes("price").setQuery(
                QueryBuilders.matchAllQuery()
        ).get();

        System.out.println("-------------------------------");
        if(response.getHits().getHits().length>0){
//            System.out.println(response.getHits().getHits().toString());
            for(SearchHit searchHit : response.getHits().getHits()){
                System.out.println(searchHit.getSourceAsString());
            }
        }
        System.out.println("----------->>>>>>>>>>>>>>>--------");




    }
}
