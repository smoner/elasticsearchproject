package com.smoner.study.elasticsearch;

import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.net.InetAddress;

/**
 * Created by fengjqc on 2018/9/1.
 */
public class TransportClient1 {

    public static final String NAME = "employee";
    public static final String TYPE = "etype";
    public static void main(String[] args) throws Exception{
        try{

            Settings settings = Settings.builder().put("cluster.name","elasticsearch").build();
            TransportClient transportClient = new PreBuiltTransportClient(settings)
                    .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"),9300));


            String json = "{\"id\":\"1\",\"code\":\"zs\",\"name\":\"张三\",\"age\":26}";
            IndexResponse indexResponse = transportClient.prepareIndex(NAME,TYPE)
                    .setSource(json)
                    .get();

            transportClient.close();
        }catch (Exception e){
            e.printStackTrace();
            String d = null;
        }
    }
}
