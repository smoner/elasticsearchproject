package com.smoner.study.elasticsearch.biprice;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang.StringUtils;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.net.InetAddress;
import java.sql.*;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by fengjqc on 2018/9/20.
 */
public class SavePriceData2ES {
    TransportClient transportClient = null;
    public static void main(String[] args) {
        SavePriceData2ES savePriceData2ES = new SavePriceData2ES();
        Connection connection = null;
        Statement statement = null ;
        try{
            long starttime = System.currentTimeMillis();
            String driverName = "org.postgresql.Driver" ;
            String dburl = "jdbc:postgresql://172.20.12.57:5432/cpu_biprice" ;
            String username = "postgres" ;
            String password = "1" ;
            Driver driver = (Driver) Class.forName(driverName).newInstance();
            DriverManager.registerDriver(driver);
            connection  = DriverManager.getConnection(dburl,username,password);
            statement = connection.createStatement();
            TransportClient transportClient = savePriceData2ES.getTransportClient();
            savePriceData2ES.insertBy500(connection,statement,savePriceData2ES,transportClient);
            long endtime = System.currentTimeMillis();
            System.out.println("--------------------总耗时:"+(endtime-starttime)+"-------------------------");
            savePriceData2ES.closeTransportClient(transportClient);
        }catch (Exception e){
            e.printStackTrace();
            String d = null;
        }finally {
                try {
                    if(connection!=null){
                        connection.close();
                    }
                    if(statement!=null){
                        statement.close();
                    }
                } catch (SQLException e) {
                    e.printStackTrace();
                    String dd = null;
                }
        }
    }
    public TransportClient getTransportClient() throws Exception{
        Settings settings =  Settings.builder().put("cluster.name","elasticsearch").build();
        if(this.transportClient!=null){
            return this.transportClient;
        }
        this.transportClient =new PreBuiltTransportClient(settings)
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"),9300));
        return this.transportClient;
    }
    public void closeTransportClient(TransportClient transportClient){
        if(null!=this.transportClient){
            this.transportClient.close();
        }
    }
    public void insertBy500(Connection connection,Statement statement,SavePriceData2ES savePriceData2ES, TransportClient transportClient)throws Exception {
        int num_all = 100000;
        int countOnce = 10000;
        for (int num = 0; num * countOnce < num_all; num++) {
            StringBuffer dataSql = new StringBuffer();
//            dataSql.append(" select * from cpu_biprice where vpurchase_erp_id = '1013' and vpurchase_code is not null ");
            dataSql.append(" select * from cpu_biprice  ");
            dataSql.append(" limit ").append(countOnce).append(" offset ").append((num) * countOnce);
            long stime1 = System.currentTimeMillis();
            ResultSet resultSet = statement.executeQuery(dataSql.toString());
            long etime1 = System.currentTimeMillis();
            System.out.println("----每次查询的时间:"+(etime1-stime1));
            long stime2 = System.currentTimeMillis();
            BulkRequestBuilder bulkRequestBuilder = transportClient.prepareBulk();
            while (resultSet.next()) {
                bulkRequestBuilder.add(transportClient.prepareIndex("yc-biprice", "price", resultSet.getString("vsrc_mark")).setSource(
                                XContentFactory.jsonBuilder().startObject()
                                        .field("id", resultSet.getLong("id"))
                                        .field("vtenant_id", resultSet.getString("vtenant_id"))
                                        .field("vpurchase_code", resultSet.getString("vpurchase_code"))
                                        .field("vpurchase_enterprise_id", resultSet.getLong("vpurchase_enterprise_id"))
                                        .field("vsupply_name", resultSet.getString("vsupply_name"))
                                        .field("vsupply_erp_id", resultSet.getString("vsupply_erp_id"))
                                        .field("vsupply_enterprise_id", resultSet.getLong("vsupply_enterprise_id"))
                                        .field("vmaterial_id", resultSet.getLong("vmaterial_id"))
                                        .field("vmaterial_code", resultSet.getString("vmaterial_code"))
                                        .field("vmaterial_name", resultSet.getString("vmaterial_name"))
                                        .field("vmaterial_type", resultSet.getString("vmaterial_type"))
                                        .field("nsum", resultSet.getLong("nsum"))
                                        .field("nprice", resultSet.getLong("nprice"))
                                        .field("vsrc_mark", resultSet.getString("vsrc_mark"))
                                        .field("ntax", resultSet.getBigDecimal("ntax"))
                                        .field("vsrc_system", resultSet.getString("vsrc_system"))
                                        .field("vadd_type", resultSet.getString("vadd_type"))
                                        .field("vsrc_billcode", resultSet.getString("vsrc_billcode"))
                                        .field("vpurchase_erp_name", resultSet.getString("vpurchase_erp_name"))
                                        .field("vpurchase_org_id", resultSet.getLong("vpurchase_org_id"))
                                        .field("vunit_name", resultSet.getString("vunit_name"))
                                        .field("jcondition", getMapValue(resultSet.getString("jcondition")))
                                        .field("jprice", getMapValue(resultSet.getString("jprice")))
                                        .field("jmaterial", getMapValue(resultSet.getString("jmaterial")))
                                        .field("jsupply", getMapValue(resultSet.getString("jsupply")))
                                        .endObject()
                        )
                );
            }
            long stime = System.currentTimeMillis();
            System.out.println("----每次转换的时间:"+(stime-stime2));
            if(bulkRequestBuilder!=null&&bulkRequestBuilder.request()!=null&&bulkRequestBuilder.request().numberOfActions()>0){
                BulkResponse bulkResponse = bulkRequestBuilder.get();
            }else{
                break;
            }
            long etime = System.currentTimeMillis();
            System.out.println("----每次的时间:"+(etime-stime));
        }
    }

    public Map<String,Object> getMapValue(String value){
        Map<String,Object> map = new HashMap<String,Object>();
        if(StringUtils.isNotBlank(value)){
            JSONObject jsonObject = JSON.parseObject(value);
            if(jsonObject!=null&&jsonObject.size()>0){
                return jsonObject.getInnerMap();
            }
        }
        return map;
    }
}
