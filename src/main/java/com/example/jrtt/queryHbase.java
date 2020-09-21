package com.example.jrtt;

import net.sf.json.JSONObject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.util.StopWatch;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class queryHbase {
    //创建连接
    public static Configuration conf;
    public static String queryTableName="tenant_jrtt_creative_get_v2";
    static {
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum","10.0.4.45");
        conf.set("hbase.zookeeper.property.clientPort","2181");
    }
    //循环分区信息，利用协处理器循环之中批量查询rowkey信息
    public List<String>  getAllRowkey() throws IOException {
        //计时
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        List<String> RowkeyList = new ArrayList<String>();
        HBaseAdmin Admin = new HBaseAdmin(conf);
        boolean tenant_jrtt_ad_get_v2 = Admin.tableExists(queryTableName);
        TableName tableName = TableName.valueOf(queryTableName);
        HTable hTable = new HTable(conf, queryTableName);
        Scan scan = new Scan();
        for (int i=0;i <= 9;i++){
            scan.setStartRow(Bytes.toBytes(i + "|" + "20200727" + "|0"));
            scan.setStopRow(Bytes.toBytes(i + "|" + "20200727" + "|z"));
            ResultScanner resultScanner = hTable.getScanner(scan);
            for (Result result : resultScanner) {
                Cell[] cells = result.rawCells();
                for (Cell cell : cells) {
                    //得到 rowkey
                    String s = Bytes.toString(CellUtil.cloneRow(cell));
                    System.out.println(s);
                    RowkeyList.add(s);
                }
            }
        }
        System.out.println("RowkeyList的长度为： "+RowkeyList.size());
        stopWatch.stop();
        System.out.println("统计耗时："+(stopWatch.getTotalTimeMillis())/1000+"秒");
        return RowkeyList;
    }
    //循环完毕，用List去查Hbase，对比有无异常数据
    public Map<String, JSONObject> getAllJson(List<String> RowkeyList) throws IOException {
        HashMap<String, JSONObject> jsonMap = new HashMap<>();
//        HTable hTable = new HTable(conf, queryTableName);
//        HTable hTable2 = new HTable(conf, queryTableName);
//        for (String rowKey:RowkeyList){
//            Get get = new Get(Bytes.toBytes(rowKey));
//            Result result = hTable.get(get);
//            for (Cell cell:result.rawCells()){
//                String s = Bytes.toString(CellUtil.cloneValue(cell));
//                JSONObject valueJson = JSONObject.fromObject(s);
// //               jsonMap.put(rowKey,valueJson);
//                System.out.println("rowKey = "+rowKey);
//                System.out.println("valueJson"+valueJson);
//            }
//        }

        return jsonMap;
    }
}