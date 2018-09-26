package com.smoner.study.elasticsearch;


import com.fasterxml.jackson.databind.ObjectMapper;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.io.Serializable;
import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by fengjqc on 2018/9/1.
 */
public class IndexResponseTest implements Serializable {
    public static final String NAME = "employee";
    public static final String TYPE = "etype";
    public static void main(String[] args){
        try{
            IndexResponseTest indexResponseTest = new IndexResponseTest();
            Settings settings = Settings.builder().put("cluster.name","elasticsearch").build();
            TransportClient transportClient = new PreBuiltTransportClient(settings)
                    .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"),9300));
            BulkRequestBuilder bulkRequestBuilder = transportClient.prepareBulk();
            bulkRequestBuilder.add(transportClient.prepareIndex(NAME,TYPE)
                    .setSource(indexResponseTest.getJsonBySerializable()));
            bulkRequestBuilder.add(transportClient.prepareIndex(NAME,TYPE)
                    .setSource(indexResponseTest.getJsonByMap()));
            BulkResponse bulkResponse = bulkRequestBuilder.get();
            if(bulkResponse.hasFailures()){
                //失败处理
            }
            transportClient.close();
        }catch (Exception e){
            throw new RuntimeException(e.getMessage());
        }
    }
    public Map<String,Object> getJsonByMap(){
        Map<String,Object> map = new HashMap<String, Object>();
        map.put("id","3");
        map.put("name","王五");
        map.put("code","ww");
        map.put("age",28);
        return map;
    }
    public byte[] getJsonBySerializable(){
        try {
            Person p = new Person();
            p.setId("2");
            p.setName("李四");
            p.setCode("ls");
            p.setAge(27);
            ObjectMapper objectMapper = new ObjectMapper();
            return objectMapper.writeValueAsBytes(p);
        }catch (Exception e){
            throw new RuntimeException(e.getMessage());
        }
    }
    class Person implements Serializable{
        String id  = null;
        String code = null;
        String name = null;
        int age = 0;

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public String getCode() {
            return code;
        }

        public void setCode(String code) {
            this.code = code;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public int getAge() {
            return age;
        }

        public void setAge(int age) {
            this.age = age;
        }
    }
}
