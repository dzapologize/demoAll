package com.example.jrtt;


import com.alibaba.fastjson.JSONObject;
import com.example.jrtt.Util.deleteTxtFile;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.util.StopWatch;

import java.io.*;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;


public class sameRowkeyOtherValue {
    //创建连接
    public static Configuration conf;

    static {
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "10.0.4.45");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
    }

    public static String tableName =
//    				"tenant_jrtt_advertiser_info_v2";//广告主信息gjx
              "tenant_jrtt_child_advertiser_info_v2";//广告主信息二代账户gjx/下午跑有数据
//				"tenant_jrtt_qualification_get_v2";//获取广告主资质gjx/表名不一致
//				"tenant_jrtt_advertiser_select_v2";//广告主列表lzy
//				"tenant_jrtt_child_agent_select_v2";//二级代理商列表lzy
//				"tenant_jrtt_advertiser_budget_get";//查询账号余额lzt账户余额已上线
//				"tenant_jrtt_one_agent_fund_v2";//查询账户余额-一级代理商lzt
//				"tenant_jrtt_agent_fund_v2";//查询账号余额-二代代理商lzt
//				"tenant_jrtt_advertiser_fund_daily_stat_v2";//查询账号日流水lzt
//            "tenant_jrtt_adv_fund_detail_v2";//查询账号流水明细lzy
    public static String testTableName = "parametric_test:" + tableName;
    //    public static String startTime = "20200805";
//    public static String endTime = "20200805";
    public static String startTime = "20200809";
    public static String endTime = "20200809";

    public static void main(String[] args) throws IOException {
        //正式表全量rowkey
        //计时
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        HashMap<String, String> rowkeyValue = new HashMap<>();
        HBaseAdmin Admin = new HBaseAdmin(conf);
        boolean tenant_jrtt_ad_get_v2 = Admin.tableExists(tableName);
//        TableName tableName = TableName.valueOf("tenant_jrtt_advertiser_fund_get_v2");
        HTable hTable = new HTable(conf, tableName);
        Scan scan = new Scan();
        for (int i = 0; i <= 9; i++) {
            scan.setStartRow(Bytes.toBytes(i + "|" + startTime + "|0"));
            scan.setStopRow(Bytes.toBytes(i + "|" + endTime + "|z"));
            ResultScanner resultScanner = hTable.getScanner(scan);
            for (Result result : resultScanner) {
                Cell[] cells = result.rawCells();
                for (Cell cell : cells) {
                    //得到 rowkey
                    String rowkey = Bytes.toString(CellUtil.cloneRow(cell));
                    //得到value
                    String value = Bytes.toString(CellUtil.cloneValue(cell));
                    JSONObject jsonObject = JSONObject.parseObject(value);
                    String data = jsonObject.getString("data");
                    System.out.println("生产表的data:" + data);
                    rowkeyValue.put(rowkey, data);
                }
            }
        }
        ////测试表全量rowkey
        HashMap<String, String> testRowkeyValue = new HashMap<>();
        TableName tableName23 = TableName.valueOf(testTableName);
        HTable hTable23 = new HTable(conf, testTableName);
        Scan scan23 = new Scan();
        for (int i = 0; i <= 9; i++) {
            scan23.setStartRow(Bytes.toBytes(i + "|" + startTime + "|0"));
            scan23.setStopRow(Bytes.toBytes(i + "|" + endTime + "|z"));
            ResultScanner resultScanner = hTable23.getScanner(scan23);
            for (Result result : resultScanner) {
                Cell[] cells = result.rawCells();
                for (Cell cell : cells) {
                    //得到 rowkey
                    String rowkey = Bytes.toString(CellUtil.cloneRow(cell));
                    //得到value
                    String value = Bytes.toString(CellUtil.cloneValue(cell));
                    JSONObject jsonObject = JSONObject.parseObject(value);
                    String data = jsonObject.getString("data");
                    testRowkeyValue.put(rowkey, data);
                }
            }
        }
        //对比同一个Rowkey时value的差异
        HashMap<String, HashMap<String, String>> otherRowkeyValue = new HashMap<>();
        HashMap<String, String> other = new HashMap<>();
        int sum = 0;
        if (rowkeyValue.size() >= testRowkeyValue.size()) {
            for (String key : rowkeyValue.keySet()) {
                String Value = rowkeyValue.get(key);
                String testValue = testRowkeyValue.get(key);
                if (Value.equals(testValue)) {
//                    System.out.println(testkey+Value+testValue);
                } else {
                    other.put(Value, testValue);
                    otherRowkeyValue.put(key, other);
                    System.out.println("otherotherother:   "+other);
                    other.clear();
                    sum = sum + 1;
                }
            }
        } else {
            for (String key : testRowkeyValue.keySet()) {
                String Value = rowkeyValue.get(key);
                String testValue = testRowkeyValue.get(key);
                if (Value.equals(testValue)) {
//                    System.out.println(key + Value + testValue);
                }else {
                    other.put(Value, testValue);
                    otherRowkeyValue.put(key, other);
                    System.out.println("otherotherother:   "+other);
                    other.clear();
                    sum = sum + 1;
                }
            }
        }
        stopWatch.stop();
        System.out.println("正式表" + tableName + "的RowkeyList的长度为： " + rowkeyValue.size());
        System.out.println("测试表" + testTableName + "的RowkeyList的长度为： " + testRowkeyValue.size());
        System.out.println("当时间为" + startTime + "~" + endTime + "时两表差异的rowkey量为：" + sum);
        //把不同的data数据存到txt里
        deleteTxtFile deleteTxtFile = new deleteTxtFile();
        try {
            File file=new File("D:\\txt\\");  //  /传参是路径名
            //删除txt文件
            deleteTxtFile.delete(file);
            String line = System.getProperty("line.separator");
            StringBuffer str = new StringBuffer();
            //创建txt文件
            FileWriter fw = new FileWriter("D:\\txt\\otherValue.txt", true);
            Set<Map.Entry<String, HashMap<String, String>>> set = otherRowkeyValue.entrySet();
            Iterator<Map.Entry<String, HashMap<String, String>>> iterator = set.iterator();
            fw.flush();
            while (iterator.hasNext()) {
                Map.Entry<String, HashMap<String, String>> next = iterator.next();
                HashMap<String, String> value = next.getValue();
//                System.out.println("valueString"+value);
                str.append(next.getKey() + "   :   " + next.getValue()).append(line);
            }
            fw.write(str.toString());
            fw.close();
        }catch (Exception e){
            e.printStackTrace();
        }
        System.out.println(otherRowkeyValue);
    }
}
