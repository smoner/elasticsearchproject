package com.smoner.study.elasticsearch.biprice;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.script.mustache.SearchTemplateRequestBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by fengjqc on 2018/9/26.
 */
public class BiPriceTemplateSaveService {
    public static void main(String[] args) throws Exception{
        Settings settings = Settings.builder().put("cluster.name","elasticsearch").build();
        TransportClient transportClient = new PreBuiltTransportClient(settings).addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"),9300));

        transportClient.admin().cluster().preparePutStoredScript()
                .setLang("mustache")
                .setId("ycbiprice-temp-01")
                .setContent(new BytesArray("{\"template\": {\"query\": {\"match\": {\"vsupply_name.keyword\": \"{{query_string}}\"}}}}"), XContentType.JSON)
                .get();

        Map<String,Object> map = new HashMap<String,Object>();
        map.put("query_string","唐山君安仪器仪表有限公司");
        SearchResponse searchResponse = new SearchTemplateRequestBuilder(transportClient)
                .setScript("ycbiprice-temp-01")
                .setScriptType(ScriptType.STORED)
                .setScriptParams(map)
                .setRequest(new SearchRequest("yc-biprice").types("price"))
                .get()
                .getResponse();

        if (searchResponse.getHits().getHits().length>0){
            for(SearchHit searchHit : searchResponse.getHits().getHits()){
                System.out.println(searchHit.getSourceAsString());
            }
        }

    }
}
